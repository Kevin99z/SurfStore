package surfstore

import (
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

func ceilDiv(a int, b int) int { //ceil(a/b)
	return (a + b - 1) / b
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func pullFile(client RPCClient, remoteMeta *FileMetaData, localMeta *FileMetaData, blockStoreAddr string, wg *sync.WaitGroup) {
	blocks := make([]Block, len(remoteMeta.BlockHashList))
	for i, hash := range remoteMeta.BlockHashList {
		err := client.GetBlock(hash, blockStoreAddr, &blocks[i])
		if err != nil {
			log.Println("[client] Error fetching block")
			wg.Done()
			return
		}
	}
	f, err := os.OpenFile(path.Join(client.BaseDir, remoteMeta.Filename), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	for _, b := range blocks {
		f.Write(b.BlockData)
	}
	f.Close()
	localMeta.Version = remoteMeta.Version
	localMeta.BlockHashList = remoteMeta.BlockHashList
	wg.Done()
}

func pushFile(client RPCClient, localMeta *FileMetaData, blockStoreAddr string, w_chan chan<- string) {
	success := true
	if localMeta.BlockHashList[0] != TOMBSTONE_HASHVALUE { //push file blocks if it exists
		txt, err := os.ReadFile(path.Join(client.BaseDir, localMeta.Filename))
		if err != nil {
		}
		// push to BlockStore
		bs := client.BlockSize
		for i := 0; i < len(txt); i += bs {
			err := client.PutBlock(&Block{BlockSize: int32(bs), BlockData: txt[i:min(i+bs, len(txt))]}, blockStoreAddr, &success)
			if err != nil {
				log.Println("[client] Error pushing block")
			}
		}
	}
	if success {
		// push to MetaStore
		var latestVersion int32
		client.UpdateFile(localMeta, &latestVersion)
		if latestVersion == -1 { // server has new version of file, needs pull
			w_chan <- localMeta.Filename
			success = false
		}
	}
	if !success {
		localMeta.Version -= 1
	}
	w_chan <- ""
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	log.Println("[Client] Start Syncing")
	baseDir := client.BaseDir
	blockSize := client.BlockSize
	var blockStoreAddr string
	err := client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		log.Fatal(err)
	}
	// scan files
	files, err := os.ReadDir(baseDir)
	if err != nil {
		log.Fatal(err)
	}
	// calculate hashes for each file
	localFileHashList := make(map[string][]string)
	for _, file := range files {
		if file.IsDir() || file.Name() == "index.db" {
			continue
		}
		path := path.Join(baseDir, file.Name())
		txt, err := os.ReadFile(path)
		if err != nil {
			log.Fatal(err)
		}
		blockNum := ceilDiv(len(txt), blockSize)
		localFileHashList[file.Name()] = make([]string, blockNum)
		for i := 0; i < blockNum; i++ {
			localFileHashList[file.Name()][i] = GetBlockHashString(txt[i*blockSize : min((i+1)*blockSize, len(txt))])
		}
	}
	// get local index
	localFileMetaMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Fatal(err)
	}

	// compare files with local index
	modifiedFiles := make(map[string]int32) //map file name to new version number
	for fileName, hashList := range localFileHashList {
		// check if there are new files in baseDir
		if localMeta, exist := localFileMetaMap[fileName]; !exist { //new file
			modifiedFiles[fileName] = 1
			localFileMetaMap[fileName] = &FileMetaData{Version: 0} // initialize new file in local index to pass check before push
		} else if strings.Join(localMeta.BlockHashList, HASH_DELIMITER) != strings.Join(hashList, HASH_DELIMITER) { //modified
			modifiedFiles[fileName] = localMeta.Version + 1
		}
	}
	// check deleted files
	for fileName, localMeta := range localFileMetaMap {
		if _, exist := localFileHashList[fileName]; !exist {
			modifiedFiles[fileName] = localMeta.Version + 1
		}
	}

	// get remote index
	var remoteFileMetaMap map[string]*FileMetaData
	err = client.GetFileInfoMap(&remoteFileMetaMap)
	if err != nil {
		log.Fatal(err)
	}

	// pull files from remote
	wg := sync.WaitGroup{}
	for file, remoteMeta := range remoteFileMetaMap {
		localMeta, hasMeta := localFileMetaMap[file]
		if !hasMeta || (hasMeta && localMeta.Version < remoteMeta.Version) {
			newMeta := FileMetaData{Filename: file, Version: 0, BlockHashList: []string{EMPTYFILE_HASHVALUE}}
			localFileMetaMap[file] = &newMeta
			wg.Add(1)
			go pullFile(client, remoteFileMetaMap[file], &newMeta, blockStoreAddr, &wg)
		}
	}
	wg.Wait()

	// push modified files
	bidirect_chan := make(chan string)
	cnt := 0
	for fileName, version := range modifiedFiles {
		if version == localFileMetaMap[fileName].Version+1 {
			hashList, ok := localFileHashList[fileName]
			if !ok { // file deleted
				hashList = []string{TOMBSTONE_HASHVALUE}
			}
			newMeta := FileMetaData{Filename: fileName, Version: version, BlockHashList: hashList}
			localFileMetaMap[fileName] = &newMeta
			cnt += 1
			go pushFile(client, &newMeta, blockStoreAddr, bidirect_chan)
		}
	}
	var filesToPull []string
	for ; cnt > 0; cnt-- {
		tmp := <-bidirect_chan
		if len(tmp) > 0 {
			filesToPull = append(filesToPull, tmp)
		}
	}

	if len(filesToPull) > 0 { // finish syncing modified files
		// get remote index again
		err = client.GetFileInfoMap(&remoteFileMetaMap)
		if err != nil {
			log.Fatal(err)
		}
		// pull files from remote
		for _, file := range filesToPull {
			newMeta := FileMetaData{Filename: file, Version: 0, BlockHashList: []string{EMPTYFILE_HASHVALUE}}
			localFileMetaMap[file] = &newMeta
			wg.Add(1)
			go pullFile(client, remoteFileMetaMap[file], &newMeta, blockStoreAddr, &wg)
		}
		wg.Wait()
	}

	// write index.db
	WriteMetaFile(localFileMetaMap, baseDir)
}
