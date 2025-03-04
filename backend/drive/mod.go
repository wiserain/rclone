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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	drive_v2 "google.golang.org/api/drive/v2"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/driveactivity/v2"
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

// ------------------------------------------------------------

func (f *Fs) activityNotify(ctx context.Context, notifyFunc func(string, fs.EntryType), pollIntervalChan <-chan time.Duration) {
	go func() {
		// get the `startTime` early so all changes from now on get processed
		startTime := strconv.FormatInt(time.Now().UnixMilli(), 10)
		var err error
		var ticker *time.Ticker
		var tickerC <-chan time.Time
		for {
			select {
			case pollInterval, ok := <-pollIntervalChan:
				if !ok {
					if ticker != nil {
						ticker.Stop()
					}
					return
				}
				if ticker != nil {
					ticker.Stop()
					ticker, tickerC = nil, nil
				}
				if pollInterval != 0 {
					ticker = time.NewTicker(pollInterval)
					tickerC = ticker.C
				}
			case <-tickerC:
				if startTime == "" {
					startTime = strconv.FormatInt(time.Now().UnixMilli(), 10)
				}
				startTime, err = f.activityNotifyRunner(ctx, notifyFunc, startTime)
				if err != nil {
					fs.Infof(f, "Activity notify listener failure: %s", err)
				}
			}
		}
	}()
}

// parseTarget extracts file/directory information from driveactivity.Target and driveactivity.TargetReference
func parseTarget(target any) (id, name, mimeType string, isDir bool) {
	switch item := target.(type) {
	case *driveactivity.TargetReference:
		if item.DriveItem != nil {
			id = strings.TrimPrefix(item.DriveItem.Name, "items/")
			name = item.DriveItem.Title
			isDir = item.DriveItem.DriveFile != nil
			return
		}
		itemJson, _ := item.MarshalJSON()
		fs.Infof(nil, "driveactivity: unexpected TargetReference: %s", string(itemJson))
	case *driveactivity.Target:
		if item.DriveItem != nil {
			id = strings.TrimPrefix(item.DriveItem.Name, "items/")
			name = item.DriveItem.Title
			isDir = item.DriveItem.DriveFile != nil
			mimeType = item.DriveItem.MimeType
			return
		}
		itemJson, _ := item.MarshalJSON()
		fs.Infof(nil, "driveactivity: unexpected Target: %s", string(itemJson))
	}
	return
}

// parseActivity extracts file/directory change information from a Drive activity record.
func (f *Fs) parseActivity(ctx context.Context, activity *driveactivity.DriveActivity) (actionType, oldPath, newPath string, isDir bool) {
	// parse action detail
	actDetail := activity.PrimaryActionDetail
	if actDetail == nil {
		return
	}

	var (
		oldName, newName       string
		oldParents, newParents []string
	)

	switch {
	case actDetail.Create != nil: // new, upload, copy
		actionType = "CREATE"
		// can obtain parent info from a list of action
		for _, act := range activity.Actions {
			if act.Detail.Move != nil {
				for _, ref := range act.Detail.Move.AddedParents {
					if parentId, _, _, _ := parseTarget(ref); parentId != "" {
						newParents = append(newParents, parentId)
					}
				}
			}
		}
	case actDetail.Edit != nil:
		actionType = "EDIT"
	case actDetail.Move != nil:
		actionType = "MOVE"
		for _, ref := range actDetail.Move.RemovedParents {
			if parentId, _, _, _ := parseTarget(ref); parentId != "" {
				oldParents = append(oldParents, parentId)
			}
		}
		for _, ref := range actDetail.Move.AddedParents {
			if parentId, _, _, _ := parseTarget(ref); parentId != "" {
				newParents = append(newParents, parentId)
			}
		}
	case actDetail.Rename != nil:
		actionType = "RENAME"
		oldName = f.opt.Enc.ToStandardName(actDetail.Rename.OldTitle)
		newName = f.opt.Enc.ToStandardName(actDetail.Rename.NewTitle)
	case actDetail.Delete != nil:
		actionType = "DELETE"
	case actDetail.Restore != nil:
		actionType = "RESTORE"
	default:
		// permissionChange
		// comment
		// dlpChange
		// reference
		// settingsChange
		// appliedLabelChange
		return
	}

	// parse target info assuming a single driveItem
	if len(activity.Targets) != 1 {
		actJson, _ := activity.MarshalJSON()
		fs.Infof(nil, "driveactivity: more than one activity targets: %s", actJson)
		return
	}
	fileId, fileName, _, isDir := parseTarget(activity.Targets[0])
	if fileId == "" || fileName == "" {
		actJson, _ := activity.MarshalJSON()
		fs.Infof(nil, "driveactivity: empty target id or name: %s", actJson)
		return
	}
	if oldName == "" {
		oldName = f.opt.Enc.ToStandardName(fileName)
	}
	if newName == "" {
		newName = f.opt.Enc.ToStandardName(fileName)
	}

	// find the old path to clear that is already on existing file/dir tree
	if dirPath, ok := f.dirCache.GetInv(fileId); ok {
		// this will cover (move,rename,delete) of existing dirs
		oldPath = dirPath
	} else {
		for _, parent := range oldParents {
			if parentPath, ok := f.dirCache.GetInv(parent); ok {
				oldPath = path.Join(parentPath, oldName)
			}
		}
	}
	if oldPath != "" {
		if isDir {
			f.dirCache.FlushDir(oldPath)
		}
		if actionType == "DELETE" {
			return
		}
	}
	if actionType == "DELETE" && actDetail.Delete.Type == "PERMANENT_DELETE" {
		// FIXME no way to cover permanently delete case
		return
	}

	// find the new path
	if oldPath != "" && (actionType == "EDIT" || actionType == "RENAME") {
		// newParent == oldParent
		parentPath, _ := dircache.SplitPath(oldPath)
		newPath = path.Join(parentPath, newName)
	} else {
		if len(newParents) == 0 {
			// (create,restore) dirs
			// (edit,rename,delete,restore) files
			file, err := f.getFile(ctx, fileId, "parents")
			if err != nil {
				fs.Infof(nil, "driveactivity: failed to get file info: %v", err)
			} else {
				newParents = append(newParents, file.Parents...)
			}
		}
		// translate the parent dir of this object
		if len(newParents) > 0 {
			for _, parent := range newParents {
				if parentPath, ok := f.dirCache.GetInv(parent); ok {
					// and append the drive file name to compute the full file name
					newPath = path.Join(parentPath, newName)
					// this will now clear the actual file too
				}
			}
		} else { // a true root object that is changed
			newPath = newName
		}
	}
	if newPath != "" && isDir {
		f.dirCache.Put(newPath, fileId)
	}
	return
}

// activityNotifyRunner for a given request
func (f *Fs) _activityNotifyRunner(ctx context.Context, notifyFunc func(string, fs.EntryType), req *driveactivity.QueryDriveActivityRequest) (err error) {
	pageToken := ""
	for {
		req.PageToken = pageToken

		var info *driveactivity.QueryDriveActivityResponse
		err = f.pacer.Call(func() (bool, error) {
			queryCall := f.actSvc.Activity.Query(req)
			info, err = queryCall.Context(ctx).Do()
			return f.shouldRetry(ctx, err)
		})
		if err != nil {
			return err
		}

		type entryToClear struct {
			path      string
			entryType fs.EntryType
		}
		var pathsToClear []entryToClear
		for _, activity := range info.Activities {
			actType, oldPath, newPath, isDir := f.parseActivity(ctx, activity)
			fs.Infof(nil, "driveactivity %s: %q -> %q", actType, oldPath, newPath)
			entryType := fs.EntryDirectory
			if !isDir {
				entryType = fs.EntryObject
			}
			pathsToClear = append(pathsToClear, entryToClear{path: oldPath, entryType: entryType})
			pathsToClear = append(pathsToClear, entryToClear{path: newPath, entryType: entryType})
		}

		visitedPaths := make(map[string]bool)
		for _, entry := range pathsToClear {
			if entry.path == "" {
				continue
			}
			parentPath, _ := dircache.SplitPath(entry.path)
			if visitedPaths[parentPath] {
				continue
			}
			visitedPaths[parentPath] = true
			notifyFunc(entry.path, entry.entryType)
		}

		if info.NextPageToken == "" {
			return nil
		}
		pageToken = info.NextPageToken
	}
}

func (f *Fs) activityNotifyRunner(ctx context.Context, notifyFunc func(string, fs.EntryType), startTime string) (endTime string, err error) {
	endTime = strconv.FormatInt(time.Now().UnixMilli(), 10)
	req := &driveactivity.QueryDriveActivityRequest{
		Filter: fmt.Sprintf("time > %s AND time <= %s", startTime, endTime),
	}
	if f.opt.ListChunk > 0 {
		req.PageSize = f.opt.ListChunk
	}
	for nth, target := range f.opt.ActivityTargets {
		if nth > 0 {
			time.Sleep(time.Duration(f.opt.ActivitySleep))
		}
		req.AncestorName = "items/" + target
		fs.Debugf(f, "Checking for activities on %q", req.AncestorName)
		err = f._activityNotifyRunner(ctx, notifyFunc, req)
		if err != nil {
			return
		}
	}
	return
}
