package surfstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var blockAdd string
	err := client.GetBlockStoreAddr(&blockAdd)
	check(err)
	// 1. GET THE LOCAL FILE INFO
	dir, err := ioutil.ReadDir(client.BaseDir)
	check(err)
	// build a file_name -> hash map and file_name -> Block map
	dirMap := make(map[string][]string)
	//dirBlockMap := make(map[string][]*Block)
	// dirFile.Name is relative path
	for _, dirFile := range dir {
		if dirFile.Name() != "index.txt" {
			dirMap[dirFile.Name()] = getHashArray(client, dirFile.Name())
		}
	}
	//fmt.Println("divide check")
	//fmt.Println(dirMap)

	// 2. GET THE LOCAL INDEX INFO from index.txt
	indexMap := make(map[string]*FileMetaData)
	indexPath := ConcatPath(client.BaseDir, "index.txt")
	_, err = os.Stat(indexPath)
	// check whether the index.txt exist
	if err != nil {
		// when there is no index.txt in dir
		if os.IsNotExist(err) {
			os.Create(ConcatPath(client.BaseDir, "index.txt"))
		} else {
			check(err)
		}
	} else {
		indexMap, err = LoadMetaFromMetaFile(client.BaseDir)
		check(err)
	}

	// 3. GET THE SERVER INFO FROM SERVER
	serverFileInfoMap := make(map[string]*FileMetaData)

	err = client.GetFileInfoMap(&serverFileInfoMap)
	check(err)

	//fmt.Println("dirMap: ")
	//fmt.Println(dirMap)
	//fmt.Println("indexMap: ")
	// for indexKey, indexElement := range indexMap {
	// 	fmt.Println(indexKey, indexElement.BlockHashList)
	// }
	// fmt.Println("serverMap: ")
	// for serverKey, serverElement := range serverFileInfoMap {
	// 	fmt.Println(serverKey, serverElement.BlockHashList)
	// }

	// go through server file
	for serverKey, serverElement := range serverFileInfoMap {
		_, inLocal := dirMap[serverKey]
		_, inIdx := indexMap[serverKey]
		// Situation: x x v or v x v
		if !inIdx {
			fmt.Println("Situation x x v: download anyway")
			CreateLocalFile(client, blockAdd, serverKey, serverElement.BlockHashList)
			indexMap[serverKey] = serverFileInfoMap[serverKey]
		}

		// Situation: x v v, delete directly or tombstone
		if !inLocal && inIdx {
			idxMeta := indexMap[serverKey]
			if idxMeta.Version == serverElement.Version && len(idxMeta.BlockHashList) == 1 && idxMeta.BlockHashList[0] == "0" {
				fmt.Println("Situation x v v: tombstone not update Version")
				continue
			}
			fmt.Println("Situation x v v: delete and update Version")
			var newHashList []string
			newHashList = append(newHashList, "0")
			updateFileMetaData := &FileMetaData{
				Filename:      serverKey,
				Version:       indexMap[serverKey].Version + 1,
				BlockHashList: newHashList,
			}
			latestVersion := new(int32)
			// Situation x v v: delete successful
			err = client.UpdateFile(updateFileMetaData, latestVersion)
			check(err)
			var zero int32 = 0
			// Situation x v v: delete unsuccessful
			if *latestVersion < zero {
				err = client.GetFileInfoMap(&serverFileInfoMap)
				check(err)
				CreateLocalFile(client, blockAdd, serverKey, serverFileInfoMap[serverKey].BlockHashList)
				indexMap[serverKey] = serverFileInfoMap[serverKey]
			}
		}
	}

	// update dir file map
	dir, err = ioutil.ReadDir(client.BaseDir)
	check(err)
	for _, dirFile := range dir {
		if dirFile.Name() != "index.txt" {
			dirMap[dirFile.Name()] = getHashArray(client, dirFile.Name())
		}
	}

	// go through dir file
	var debugRound int = 0
	for dirKey, dirElement := range dirMap {
		debugRound += 1
		//fmt.Println("this is: ", dirKey)
		//fmt.Println(debugRound, dirKey)
		_, inIdx := indexMap[dirKey]
		_, inServer := serverFileInfoMap[dirKey]
		// Situdation: v x x, upload directly
		if !inServer {
			fmt.Println(dirKey, "Situation v x x")
			dirBlockArr := getBlockArray(client, dirKey)
			for _, newBlocks := range dirBlockArr {
				var succ bool
				err = client.PutBlock(newBlocks, blockAdd, &succ)
				check(err)
			}
			indexMap[dirKey] = &FileMetaData{
				Filename:      dirKey,
				Version:       1,
				BlockHashList: dirElement,
			}
			latestVersion := new(int32)
			err = client.UpdateFile(indexMap[dirKey], latestVersion)
			check(err)
			var zero int32 = 0
			// upload fail
			if *latestVersion < zero {
				err = client.GetFileInfoMap(&serverFileInfoMap)
				check(err)
				CreateLocalFile(client, blockAdd, dirKey, serverFileInfoMap[dirKey].BlockHashList)
			}
		}

		// Situation: v v v
		if inIdx && inServer {
			fmt.Println("Situation v v v")
			//fmt.Println(dirElement)
			//fmt.Println(indexMap[dirKey].GetBlockHashList())
			if !Equal(dirElement, indexMap[dirKey].GetBlockHashList()) {
				fmt.Println("Situation v v v: Not Equal")
				indexVersion := indexMap[dirKey].Version
				serverVersion := serverFileInfoMap[dirKey].Version
				// Situation v v v: update file
				if indexVersion == serverVersion {
					fmt.Println("Situation v v v: Upload Blocks, not successful")
					dirBlockArr := getBlockArray(client, dirKey)
					for _, uploadBlocks := range dirBlockArr {
						var succ bool
						err = client.PutBlock(uploadBlocks, blockAdd, &succ)
						check(err)
					}
				}
				updateFileMetaData := &FileMetaData{
					Filename:      dirKey,
					Version:       indexVersion + 1,
					BlockHashList: dirElement,
				}
				latestVersion := new(int32)
				err = client.UpdateFile(updateFileMetaData, latestVersion)
				check(err)
				var zero int32 = 0
				if *latestVersion > zero {
					// Situation v v v: file update successful
					fmt.Println("Situation v v v: Upload Blocks, update meta file")
				} else {
					// Situation v v v: file update unsuccessful
					err = client.GetFileInfoMap(&serverFileInfoMap)
					check(err)
					CreateLocalFile(client, blockAdd, dirKey, serverFileInfoMap[dirKey].BlockHashList)
					fmt.Println("Situation v v v: Upload fails")
				}
			} else {
				indexVersion := indexMap[dirKey].Version
				serverVersion := serverFileInfoMap[dirKey].Version
				if serverVersion > indexVersion {
					// deleteFilePath := ConcatPath(client.BaseDir, dirKey)
					// os.Remove(deleteFilePath)
					err = client.GetFileInfoMap(&serverFileInfoMap)
					check(err)
					CreateLocalFile(client, blockAdd, dirKey, serverFileInfoMap[dirKey].BlockHashList)
				}
			}
		}
		if !inIdx && inServer {
			fmt.Println("Situation v x v: download anyway")
			CreateLocalFile(client, blockAdd, dirKey, serverFileInfoMap[dirKey].BlockHashList)
			indexMap[dirKey] = serverFileInfoMap[dirKey]
		}
	}
	// update FileInfo information to the local index
	//PrintMetaMap(indexMap)
	err = client.GetFileInfoMap(&indexMap)
	check(err)
	//PrintMetaMap(indexMap)
	err = WriteMetaFile(indexMap, client.BaseDir)
	check(err)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// file_name -> *Block Array and file_name -> BlockHashArray
func Divide(client RPCClient, fileName string) ([]*Block, []string) {
	var blockHashList []string
	var blockArr []*Block
	filePath := ConcatPath(client.BaseDir, fileName)
	f, err := os.Open(filePath)
	check(err)
	for {
		buffer := make([]byte, client.BlockSize)
		var curBlock Block
		n, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				blockHashList = append(blockHashList, GetBlockHashString(buffer[:n]))
				curBlock.BlockData = buffer[:n]
				curBlock.BlockSize = int32(n)
				blockArr = append(blockArr, &curBlock)
				// fmt.Println("eof")
				// fmt.Println(blockArr)
				break
			} else {
				check(err)
			}
		} else {
			blockHashList = append(blockHashList, GetBlockHashString(buffer[:n]))
			curBlock.BlockData = buffer[:n]
			curBlock.BlockSize = int32(n)
			blockArr = append(blockArr, &curBlock)
		}
	}
	return blockArr, blockHashList
}

func getHashArray(client RPCClient, fileName string) []string {
	var blockHashList []string
	filePath := ConcatPath(client.BaseDir, fileName)
	f, err := os.Open(filePath)
	check(err)
	for {
		buffer := make([]byte, client.BlockSize)
		n, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				blockHashList = append(blockHashList, GetBlockHashString(buffer[:n]))
				break
			} else {
				check(err)
			}
		} else {
			blockHashList = append(blockHashList, GetBlockHashString(buffer[:n]))
		}
	}
	return blockHashList
}

func getBlockArray(client RPCClient, fileName string) []*Block {
	var blockArr []*Block
	filePath := ConcatPath(client.BaseDir, fileName)
	f, err := os.Open(filePath)
	check(err)
	for {
		buffer := make([]byte, client.BlockSize)
		var curBlock Block
		n, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				curBlock.BlockData = buffer[:n]
				curBlock.BlockSize = int32(n)
				blockArr = append(blockArr, &curBlock)
				break
			} else {
				check(err)
			}
		} else {
			curBlock.BlockData = buffer[:n]
			curBlock.BlockSize = int32(n)
			blockArr = append(blockArr, &curBlock)
		}
	}
	return blockArr

}

// downlad file from the server
func CreateLocalFile(client RPCClient, blockAdd string, fileName string, fileHash []string) {
	//fmt.Println(fileName)
	filePath := ConcatPath(client.BaseDir, fileName)
	_, err := os.Create(filePath)
	check(err)
	err = os.Remove(filePath)
	check(err)
	file, err := os.Create(filePath)
	check(err)
	if len(fileHash) == 1 && fileHash[0] == "0" {
		err := os.Remove(filePath)
		check(err)
	} else {
		fileByte := &Block{}
		for _, h := range fileHash {
			err := client.GetBlock(h, blockAdd, fileByte)
			check(err)
			_, err = file.Write(fileByte.BlockData)
			check(err)
		}
	}
}

// equal function compare two array
func Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
