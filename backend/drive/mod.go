package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	drive_v2 "google.golang.org/api/drive/v2"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

// ------------------------------------------------------------

// parse object id from path remote:{ID}
func parseRootID(s string) (rootID string, err error) {
	re := regexp.MustCompile(`\{([^}]{5,})\}`)
	m := re.FindStringSubmatch(s)
	if m == nil {
		return "", fmt.Errorf("%s does not contain any valid id", s)
	}
	rootID = m[1]

	if strings.HasPrefix(rootID, "http") {
		// folders - https://drive.google.com/drive/u/0/folders/
		// file - https://drive.google.com/file/d/
		re := regexp.MustCompile(`\/(folders|files|file\/d)(\/([A-Za-z0-9_-]{6,}))+\/?`)
		if m := re.FindStringSubmatch(rootID); m != nil {
			rootID = m[len(m)-1]
			return
		}
		// id - https://drive.google.com/open?id=
		re = regexp.MustCompile(`.+id=([A-Za-z0-9_-]{6,}).?`)
		if m := re.FindStringSubmatch(rootID); m != nil {
			rootID = m[1]
			return
		}
	}
	return
}

// ------------------------------------------------------------

type baseSAobject struct {
	ServiceAccountFile string
	Impersonate        string
}

type ServiceAccountPool struct {
	creds   string          // on newServiceAccountPool
	files   []string        // on newServiceAccountPool
	users   []string        // on newServiceAccountPool
	mutex   *sync.Mutex     // on newServiceAccountPool
	maxLoad int             // on newServiceAccountPool
	SAs     []*baseSAobject // on LoadSA()
	numLoad int
}

func newServiceAccountPool(opt *Options) (*ServiceAccountPool, error) {
	var saFiles, ipUsers []string
	if opt.ServiceAccountFilePath != "" {
		dirList, err := os.ReadDir(opt.ServiceAccountFilePath)
		if err != nil {
			return nil, fmt.Errorf("unable to read service_account_file_path: %w", err)
		}
		for _, v := range dirList {
			filePath := filepath.Join(opt.ServiceAccountFilePath, v.Name())
			if path.Ext(filePath) != ".json" {
				continue
			}
			saFiles = append(saFiles, filePath)
		}
		if len(saFiles) == 0 {
			return nil, fmt.Errorf("unable to locate service account files in %s", opt.ServiceAccountFilePath)
		}
		fs.Debugf(nil, "%d service account files from %q", len(saFiles), opt.ServiceAccountFilePath)
	} else if opt.ServiceAccountFile != "" {
		saFiles = append(saFiles, opt.ServiceAccountFile)
		fs.Debugf(nil, "1 service account file from %q", opt.ServiceAccountFile)
	}
	if opt.ImpersonateList != "" {
		var users []string
		if err := json.Unmarshal([]byte(opt.ImpersonateList), &users); err != nil {
			return nil, fmt.Errorf("unabled to read impersonate_list: %w", err)
		}
		ipUsers = append(ipUsers, users...)
		if len(ipUsers) == 0 {
			return nil, fmt.Errorf("unable to find impersonate users in %s", opt.ImpersonateList)
		}
		fs.Debugf(nil, "%d impersonate users from %q", len(ipUsers), opt.ImpersonateList)
	} else {
		ipUsers = append(ipUsers, opt.Impersonate)
	}
	p := &ServiceAccountPool{
		creds:   opt.ServiceAccountCredentials,
		files:   saFiles,
		users:   ipUsers,
		mutex:   new(sync.Mutex),
		maxLoad: opt.ServiceAccountMaxLoad,
	}
	// initial load
	if err := p.LoadSA(); err != nil {
		return nil, fmt.Errorf("service accout pool: initial load failed: %w", err)
	}
	return p, nil
}

func (p *ServiceAccountPool) LoadSA() error {
	if p.numLoad >= p.maxLoad {
		return fmt.Errorf("maximum service account load exceeded")
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	// make a list of baseSAobjects
	var saList []*baseSAobject
	for _, sa := range p.files {
		for _, imp := range p.users {
			saList = append(saList, &baseSAobject{
				ServiceAccountFile: sa,
				Impersonate:        imp,
			})
		}
	}
	if len(saList) == 0 && p.creds != "" {
		for _, imp := range p.users {
			saList = append(saList, &baseSAobject{
				ServiceAccountFile: "",
				Impersonate:        imp,
			})
		}
	}
	// make shuffled
	rand.Shuffle(len(saList), func(i, j int) {
		saList[i], saList[j] = saList[j], saList[i]
	})
	p.SAs = saList
	p.numLoad++
	fs.Debugf(nil, "%d service account loaded (%d/%d)", len(p.SAs), p.numLoad, p.maxLoad)
	return nil
}

func (p *ServiceAccountPool) _getSA() (newSA []*baseSAobject, err error) {
	SAs := p.SAs
	if len(SAs) == 0 {
		err = fmt.Errorf("no available service account")
		return
	}
	p.SAs, newSA = SAs[:len(SAs)-1], SAs[len(SAs)-1:]
	return newSA, nil
}

func (p *ServiceAccountPool) GetSA() (newSA []*baseSAobject, err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p._getSA()
}

// dynamically change service account if necessary
func (f *Fs) changeServiceAccount(ctx context.Context) (err error) {
	// check min sleep
	if fs.Duration(time.Since(f.changeSAtime)) < f.opt.ServiceAccountMinSleep {
		fs.Debugf(nil, "retrying with the same service account")
		return nil
	}
	f.changeSAmu.Lock()
	defer f.changeSAmu.Unlock()
	// reloading SA
	if len(f.changeSApool.SAs) < 1 {
		if err := f.changeSApool.LoadSA(); err != nil {
			return err
		}
	}
	if len(f.changeSApool.SAs) < 1 {
		return fmt.Errorf("no available service account")
	}

	sa, err := f.changeSApool.GetSA()
	if err != nil {
		return err
	}
	newOpt := f.opt
	newOpt.ServiceAccountCredentials = f.changeSApool.creds
	newOpt.ServiceAccountFile = sa[0].ServiceAccountFile
	newOpt.Impersonate = sa[0].Impersonate
	f.client, err = createOAuthClient(ctx, &newOpt, f.name, f.m)
	if err != nil {
		return fmt.Errorf("failed to create oauth client: %w", err)
	}
	f.svc, err = drive.NewService(context.Background(), option.WithHTTPClient(f.client))
	if err != nil {
		return fmt.Errorf("couldn't create Drive client: %w", err)
	}
	if f.opt.V2DownloadMinSize >= 0 {
		f.v2Svc, err = drive_v2.NewService(context.Background(), option.WithHTTPClient(f.client))
		if err != nil {
			return fmt.Errorf("couldn't create Drive v2 client: %w", err)
		}
	}
	if err == nil {
		f.changeSAtime = time.Now()
		f.pacer = fs.NewPacer(ctx, pacer.NewGoogleDrive(pacer.MinSleep(f.opt.PacerMinSleep), pacer.Burst(f.opt.PacerBurst)))
		svcAcc := "service account credential"
		if sa[0].ServiceAccountFile != "" {
			svcAcc = fmt.Sprintf("service account file \"%s\"", filepath.Base(sa[0].ServiceAccountFile))
		}
		if sa[0].Impersonate != "" {
			fs.Debugf(nil, "Now working with %s as %q", svcAcc, sa[0].Impersonate)
		} else {
			fs.Debugf(nil, "Now working with %s", svcAcc)
		}
		fs.Debugf(nil, "%d service account remaining", len(f.changeSApool.SAs))
	}
	return err
}

// ------------------------------------------------------------

type GdsClient struct {
	client *rest.Client
	userid string
	apikey string
	mode   string
}

func newGdsClient(ctx context.Context, opt *Options) (*GdsClient, bool, error) {
	ok := true
	if opt.GdsUserid == "" && opt.GdsApikey == "" {
		ok = false
	} else if opt.GdsUserid == "" || opt.GdsApikey == "" {
		return nil, ok, fmt.Errorf("required both --drive-gds-userid and --drive-gds-apikey")
	}
	gds := &GdsClient{
		client: rest.NewClient(fshttp.NewClient(ctx)).SetRoot(opt.GdsEndpoint),
		userid: opt.GdsUserid,
		apikey: opt.GdsApikey,
		mode:   opt.GdsMode,
	}
	return gds, ok, nil
}

type GdsResponse struct {
	Result string `json:"result"`
	Data   struct {
		Member json.RawMessage
		Remote *GdsRemote `json:"remote"`
	} `json:"data"`
}

type GdsRemote struct {
	SA           json.RawMessage `json:"sa"`
	RootFolderID string          `json:"root_folder_id"`
	Impersonate  string          `json:"impersonate"`
	Scope        string          `json:"scope"`
}

func (gds *GdsClient) getGdsRemote(ctx context.Context) (remote *GdsRemote, err error) {
	form := url.Values{}
	form.Set("userid", gds.userid)
	form.Set("apikey", gds.apikey)
	form.Set("mode", gds.mode)
	opts := rest.Opts{
		Method:          "POST",
		MultipartParams: form,
	}
	var info *GdsResponse
	_, err = gds.client.CallJSON(ctx, &opts, nil, &info)
	if err != nil {
		return
	}
	if info.Result != "success" {
		err = fmt.Errorf("%v", info.Result)
		return
	}
	fs.Debugf(nil, "member: %+v\n", string(info.Data.Member))
	return info.Data.Remote, nil
}

// ------------------------------------------------------------

// get an id of file or directory
func (f *Fs) getID(ctx context.Context, path string, real bool) (id string, err error) {
	if id, _ := parseRootID(path); len(id) > 6 {
		info, err := f.getFile(ctx, id, f.getFileFields(ctx))
		if err != nil {
			return "", fmt.Errorf("no such object with id %q: %w", id, err)
		}
		if real && info.ShortcutDetails != nil {
			return info.ShortcutDetails.TargetId, nil
		}
		return info.Id, nil
	}
	path = strings.Trim(path, "/")
	id, err = f.dirCache.FindDir(ctx, path, false)
	if err != nil {
		o, err := f.NewObject(ctx, path)
		if err != nil {
			return "", err
		}
		id = o.(fs.IDer).ID()
	}
	if real {
		return actualID(id), nil
	}
	return shortcutID(id), nil
}

// Change parents of objects in (f) by an ID of (dstFs)
func (f *Fs) changeParents(ctx context.Context, dstFs *Fs, dstCreate bool, srcDepth string, srcDelete bool) (o fs.Object, err error) {
	// Find source
	srcID, err := f.getID(ctx, "", true)
	if err != nil {
		return nil, fmt.Errorf("couldn't find source: %w", err)
	}
	// Find destination
	dstID, err := dstFs.dirCache.FindDir(ctx, "", dstCreate)
	if err != nil {
		return nil, fmt.Errorf("couldn't find destination: %w", err)
	}
	dstID = actualID(dstID)

	// list the objects
	infos := []*drive.File{}
	if srcDepth == "0" {
		info, err := f.getFile(ctx, srcID, "id,name,parents")
		if err != nil {
			return nil, fmt.Errorf("couldn't get source info: %w", err)
		}
		infos = append(infos, info)
	} else if srcDepth == "1" {
		_, err = f.list(ctx, []string{srcID}, "", false, false, f.opt.TrashedOnly, true, func(info *drive.File) bool {
			infos = append(infos, info)
			return false
		})
		if err != nil {
			return nil, fmt.Errorf("couldn't list source info: %w", err)
		}
	}

	// move them into place
	dstInfo := &drive.File{
		Parents:      nil,
		ModifiedTime: time.Now().Format(timeFormatOut),
	}
	for _, info := range infos {
		parentID := strings.Join(info.Parents, ",")
		parentID = actualID(parentID)
		fs.Infof(f, "changing parent of %q from %q to %q", info.Name, parentID, dstID)
		// Do the change
		err = f.pacer.Call(func() (bool, error) {
			_, err = f.svc.Files.Update(info.Id, dstInfo).
				RemoveParents(parentID).
				AddParents(dstID).
				Fields("").
				SupportsAllDrives(true).
				Context(ctx).Do()
			return f.shouldRetry(ctx, err)
		})
		if err != nil {
			return nil, fmt.Errorf("failed changing parents: %w", err)
		}
	}
	if srcDepth == "1" && srcDelete && len(infos) != 0 {
		// rmdir (into trash) the now empty source directory
		fs.Infof(f, "removing empty directory")
		err = f.delete(ctx, srcID, f.opt.UseTrash)
		if err != nil {
			return nil, fmt.Errorf("failed removing empty directory: %w", err)
		}
	}
	return nil, nil
}
