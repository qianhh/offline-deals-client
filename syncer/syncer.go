package syncer

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filedrive-team/filehelper/carv1"
	"github.com/filedrive-team/go-ds-cluster/remoteclient"
	"github.com/filedrive-team/offline-deals-client/settings"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var logging = log.Logger("syncer")

type Deal struct {
	Miner     address.Address
	Client    address.Address
	DataCid   cid.Cid
	Verified  bool
	PropCid   cid.Cid
	PieceSize abi.PaddedPieceSize
	PieceCid  cid.Cid
}

type Deals struct {
	Total int64
	List  []*Deal
}

type DataStatus int

const (
	ReadyDownload DataStatus = iota
	StartDownload
	FinishDownload
	StartImport
	FinishImport
)

type DealInfo struct {
	*Deal
	Updated  time.Time
	Status   DataStatus
	DataSize uint64
}

func NewSyncer(cachePath string, dataDir string) *Syncer {
	return &Syncer{
		cli:           &http.Client{},
		deals:         make(map[string]*DealInfo),
		dataDir:       dataDir,
		downloadQueue: make(chan *DealInfo, 1000),
		importQueue:   make(chan *DealInfo, 1000),
		cachePath:     cachePath,
	}
}

type Syncer struct {
	cli           *http.Client
	deals         map[string]*DealInfo
	dataDir       string
	downloadQueue chan *DealInfo
	importQueue   chan *DealInfo
	cachePath     string
}

func (s *Syncer) load() {
	file, err := os.Open(s.cachePath)
	if err != nil {
		if !os.IsNotExist(err) {
			logging.Fatal(err)
		}
		return
	}
	dec := gob.NewDecoder(file)
	if err = dec.Decode(&s.deals); err != nil {
		if err.Error() != "EOF" {
			logging.Errorw("load cache deals failed", "error", err)
		}
	}
	logging.Debugw("load deals", "deals", s.deals)
}

func (s *Syncer) save() {
	file, err := os.Create(s.cachePath)
	if err != nil {
		logging.Fatal(err)
	}
	defer file.Close()
	enc := gob.NewEncoder(file)
	if err = enc.Encode(s.deals); err != nil {
		logging.Errorw("save cache deals failed", "error", err)
	}
	logging.Debugw("save deals", "deals", s.deals)
}

func (s *Syncer) resetIncompleteStatus() {
	for _, di := range s.deals {
		switch di.Status {
		case StartDownload:
			di.Status = ReadyDownload
		case StartImport:
			di.Status = FinishDownload
		}
	}
}

func (s *Syncer) Run(ctx context.Context) {
	defer func() {
		s.save()
		close(s.downloadQueue)
		close(s.importQueue)
	}()

	s.load()
	s.resetIncompleteStatus()

	// TODO test
	//{"rootCid": "QmYnsC47dUxaWFeoo3bvtoARw79TtdFgAn3UDkQKo1J1NF", "pieceCid": "baga6ea4seaqjolucsdys232q5ogwmraifh4i2g7qaasy23awnu3mioqvfhri4gy", "unpaddedPieceSize": 8323072}
	dataCid, _ := cid.Decode("QmYnsC47dUxaWFeoo3bvtoARw79TtdFgAn3UDkQKo1J1NF")
	propCid, _ := cid.Decode("bafyreibf2znjlonzbpn6pwbhvugiauto2g37zv4pb4wargpoedeglo44zq")
	s.deals["test"] = &DealInfo{
		Deal: &Deal{
			DataCid: dataCid,
			PropCid: propCid,
		},
		Updated: time.Now(),
	}

	//./lotus client deal --manual-piece-cid=baga6ea4seaqjolucsdys232q5ogwmraifh4i2g7qaasy23awnu3mioqvfhri4gy --manual-piece-size=8323072 --manual-stateless-deal QmYnsC47dUxaWFeoo3bvtoARw79TtdFgAn3UDkQKo1J1NF t01000 0 518400
	//bafyreibf2znjlonzbpn6pwbhvugiauto2g37zv4pb4wargpoedeglo44zq

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case di := <-s.downloadQueue:
				switch di.Status {
				case ReadyDownload:
					if err := s.Download(ctx, di); err == nil {
						s.pushToImportQueue(di)
					} else {
						logging.Errorw("download failed", "dealinfo", di, "error", err)
						s.pushToDownloadQueue(di)
					}
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case di := <-s.importQueue:
				switch di.Status {
				case FinishDownload:
					if err := s.Import(ctx, di); err != nil {
						logging.Errorw("import failed", "dealinfo", di, "error", err)
						s.pushToImportQueue(di)
					}
				}
			}
		}
	}()

	s.processOfflineDeals(ctx)
	clientTicker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			clientTicker.Stop()
			return
		case <-clientTicker.C:
			s.processOfflineDeals(ctx)
		}
	}
}

func (s *Syncer) pushToDownloadQueue(di *DealInfo) {
	if di.Status != ReadyDownload {
		return
	}
	select {
	case s.downloadQueue <- di:
	default:
		logging.Warnw("download queue is full, push failed")
	}
}

func (s *Syncer) pushToImportQueue(di *DealInfo) {
	if di.Status != FinishDownload {
		return
	}
	select {
	case s.importQueue <- di:
	default:
		logging.Warnw("import queue is full, push failed")
	}
}

func (s *Syncer) getOfflineDeals(ctx context.Context) (*Deals, error) {
	url := strings.Join([]string{settings.Config.Api.Endpoint, "/v1/deal/offlineList"}, "")
	req, err := makeRequest(ctx, url, strings.Join([]string{"Bearer", settings.Config.Api.Token}, " "), nil)

	if err != nil {
		logging.Errorw("makeRequest failed", "url", url, "error", err)
		return nil, err
	}
	resp, err := s.cli.Do(req)
	if err != nil {
		logging.Errorf("request api failed, %v", err)
		return nil, err
	}

	body := resp.Body
	defer body.Close()

	payload, err := ioutil.ReadAll(body)
	if err != nil {
		logging.Errorf("request api failed, %v", err)
		return nil, err
	}

	type Resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data *Deals `json:"data"`
	}
	var data Resp
	if err = json.Unmarshal(payload, &data); err != nil {
		logging.Errorw("json unmarshal failed", "url", req.RequestURI, "payload", string(payload), "error", err)
		return nil, err
	}
	if data.Code != 200 {
		return nil, xerrors.New(data.Msg)
	}
	return data.Data, nil
}

func (s *Syncer) getDataAuth(ctx context.Context) (*remoteclient.Config, error) {
	url := strings.Join([]string{settings.Config.Api.Endpoint, "/v1/deal/dataAuth"}, "")
	req, err := makeRequest(ctx, url, strings.Join([]string{"Bearer", settings.Config.Api.Token}, " "), nil)

	if err != nil {
		logging.Errorw("makeRequest failed", "url", url, "error", err)
		return nil, err
	}
	resp, err := s.cli.Do(req)
	if err != nil {
		logging.Errorf("request api failed, %v", err)
		return nil, err
	}

	body := resp.Body
	defer body.Close()

	payload, err := ioutil.ReadAll(body)
	if err != nil {
		logging.Errorf("request api failed, %v", err)
		return nil, err
	}

	type Resp struct {
		Code int                  `json:"code"`
		Msg  string               `json:"msg"`
		Data *remoteclient.Config `json:"data"`
	}
	var data Resp
	if err = json.Unmarshal(payload, &data); err != nil {
		logging.Errorw("json unmarshal failed", "url", req.RequestURI, "payload", string(payload), "error", err)
		return nil, err
	}
	if data.Code != 200 {
		return nil, xerrors.New(data.Msg)
	}
	return data.Data, nil
}

func makeRequest(ctx context.Context, url string, auth string, body io.Reader) (req *http.Request, err error) {
	req, err = http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return
	}
	if auth != "" {
		req.Header.Add("Authorization", auth)
	}
	req.Header.Set("Content-Type", "application/json")
	return
}

func (s *Syncer) processOfflineDeals(ctx context.Context) {
	deals, err := s.getOfflineDeals(ctx)
	if err != nil {
		logging.Errorf("getOfflineDeals failed, %v", err)
		return
	}
	for _, deal := range deals.List {
		if di, ok := s.deals[deal.DataCid.String()]; ok {
			di.Updated = time.Now()
		} else {
			s.deals[deal.DataCid.String()] = &DealInfo{
				Deal:    deal,
				Updated: time.Now(),
			}
		}
	}
	for k, di := range s.deals {
		switch di.Status {
		case ReadyDownload:
			s.pushToDownloadQueue(di)
		case FinishDownload:
			s.pushToImportQueue(di)
		case FinishImport:
			if time.Since(di.Updated).Minutes() > settings.Config.Car.GetCleanPeriod().Minutes() {
				delete(s.deals, k)
				// delete car file
				if settings.Config.Car.AutoClean {
					path := filepath.Join(s.dataDir, fmt.Sprint(di.DataCid.String(), ".car"))
					_ = os.RemoveAll(path)
				}
				continue
			}
		}
		if time.Since(di.Updated).Hours() >= 24 {
			delete(s.deals, k)
		}
	}
	s.save()
}

func (s *Syncer) GetRemoteClient(ctx context.Context) (*remoteclient.Client, error) {
	cfg, err := s.getDataAuth(ctx)
	if err != nil {
		return nil, err
	}
	logging.Debugw("get data auth success", "config", cfg)
	return remoteclient.NewClient(ctx, cfg)
}

func (s *Syncer) Download(ctx context.Context, deal *DealInfo) error {
	path := filepath.Join(s.dataDir, fmt.Sprint(deal.DataCid.String(), ".car"))
	var err error
	deal.Status = StartDownload
	defer func() {
		if err != nil {
			deal.Status = ReadyDownload
		} else {
			deal.Status = FinishDownload
			deal.DataSize = GetSize(path)
		}
		s.save()
	}()
	cli, err := s.GetRemoteClient(ctx)
	if err != nil {
		return err
	}
	bstore := blockstore.NewBlockstore(cli.GetDataStore())

	if deal.DataSize > 0 && deal.DataSize == GetSize(path) {
		return nil
	}

	start := time.Now()
	carF, err := os.Create(path)
	if err != nil {
		logging.Errorw("create car file error", "cid", deal.DataCid.String(), "error", err)
		logging.Fatal(err)
	}
	defer carF.Close()
	err = CarExport(ctx, deal.DataCid, bstore, carF, 8)
	if err != nil {
		logging.Errorw("generate car file error", "cid", deal.DataCid.String(), "error", err)
		return err
	}
	logging.Infof("generate car file cost %v", time.Now().Sub(start))
	return nil
}

func (s *Syncer) Import(ctx context.Context, deal *DealInfo) error {
	var err error
	path := filepath.Join(s.dataDir, fmt.Sprint(deal.DataCid.String(), ".car"))
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	if deal.DataSize != GetSize(path) {
		deal.Status = ReadyDownload
		return xerrors.New("verify data size failed")
	}
	deal.Status = StartImport
	defer func() {
		if err != nil {
			deal.Status = FinishDownload
		} else {
			deal.Status = FinishImport
		}
		s.save()
	}()

	// import to market or miner
	url := getNodeApiUrl()
	rpcJsonMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "Filecoin.MarketImportDealData",
		"params":  []interface{}{deal.PropCid, path},
		"id":      3,
	}
	mJson, err := json.Marshal(rpcJsonMsg)
	if err != nil {
		logging.Errorf("json marshal param failed: %v", err)
		return err
	}
	contentReader := bytes.NewReader(mJson)
	// get auth token
	token := getNodeApiToken()
	req, err := makeRequest(ctx, url, strings.Join([]string{"Bearer", token}, " "), contentReader)
	resp, err := s.cli.Do(req)
	if err != nil {
		logging.Errorf("request rpc api failed, %v", err)
		return err
	}

	body := resp.Body
	defer body.Close()

	payload, err := ioutil.ReadAll(body)
	if err != nil {
		logging.Errorf("request rpc api failed, %v", err)
		return err
	}

	logging.Debug(string(payload))

	return nil
}

type nodeGetter struct {
	ng blockstore.Blockstore
}

func (ng *nodeGetter) Get(ctx context.Context, cid cid.Cid) (format.Node, error) {
	start := time.Now()
	defer func() {
		logging.Debugf("get node by cid %s, cost time %v", cid.String(), time.Since(start))
	}()
	nd, err := ng.ng.Get(cid)
	if err != nil {
		return nil, err
	}
	return legacy.DecodeNode(ctx, nd)
}

func (ng *nodeGetter) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	return nil
}

func CarExport(ctx context.Context, root cid.Cid, bstore blockstore.Blockstore, writer io.Writer, batchNum int) error {
	pr, pw := io.Pipe()

	nodeGetter := &nodeGetter{
		ng: bstore,
	}
	go func() {
		var outErr error
		defer func() {
			if outErr != nil {
				pw.CloseWithError(outErr)
			} else {
				pw.Close()
			}
		}()
		_, outErr = carv1.NewBatch(ctx, nodeGetter).Write(root, pw, batchNum)
	}()

	defer pr.Close()

	_, err := io.Copy(writer, pr)
	return err
}

func GetSize(path string) uint64 {
	fi, err := os.Stat(path)
	if err == nil {
		return uint64(fi.Size())
	}
	return 0
}

func getNodeConfig() (nodeType, nodeConfig string) {
	if path, ok := os.LookupEnv("LOTUS_MARKETS_PATH"); ok {
		return "markets", path
	}
	if path, ok := os.LookupEnv("LOTUS_MINER_PATH"); ok {
		return "miner or markets", path
	}
	return "miner or markets", os.Getenv("HOME") + "/.lotusminer"
}

func getNodeApiUrl() string {
	nodeType, cfg := getNodeConfig()
	path := cfg + "/api"
	content, err := ioutil.ReadFile(path)
	if err != nil {
		logging.Fatalf("cannot read %s : %v, verify the process is running %s", path, err, nodeType)
	}
	api := strings.Split(string(content), "/")
	return fmt.Sprintf("http://%s:%s/rpc/v0", api[2], api[4])
}

func getNodeApiToken() string {
	nodeType, cfg := getNodeConfig()
	path := cfg + "/token"
	content, err := ioutil.ReadFile(path)
	if err != nil {
		logging.Fatalf("cannot read %s : %v, verify the process is running %s", path, err, nodeType)
	}
	return string(content)
}
