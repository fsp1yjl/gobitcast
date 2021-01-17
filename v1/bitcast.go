package v1

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

var (
	ErrKeyNotExist = errors.New("key not exist") // kkk
)

func OpenDB(name string) *bitcast {

	path := name
	//checkout weither bitcast data dir exist, mkdir if not exist
	if _, err := os.Stat(path); err != nil {
		os.MkdirAll(path, 0755)
	}

	return &bitcast{
		name:   name,
		keyDir: make(map[string]metainfo),
	}
}

type bitcast struct {
	name         string // the absolute path of the bitcast db
	activeFid    uint   //  suffix number of active datafile
	keyDir       map[string]metainfo
	activeWriter *os.File
	maxFid       uint
}

type metainfo struct {
	fileID    uint32 // datafile suffix number
	valueSize uint32 // value  bytes length
	valuePos  uint32 // value bytes starting position in the datafile
	ts        uint32 // timestamp by second, the lastest write of the key
}

type itemHeader struct {
	crc   uint32 // the crc code of item body besides crc property itself
	ts    uint32 // timestamp by second
	kSize uint32 // key bytes length
	vSize uint32 // value bytes length
}

func joinBytes(arr ...[]byte) []byte {
	return bytes.Join(arr, nil)
}

func (h *itemHeader) toBytes() []byte {

	headerSize := int(unsafe.Sizeof(itemHeader{}))
	var x reflect.SliceHeader
	x.Len = headerSize
	x.Cap = headerSize
	x.Data = uintptr(unsafe.Pointer(h))

	retBytes := *(*[]byte)(unsafe.Pointer(&x))

	return retBytes

}

func (b *bitcast) Get(key string) (error, string) {
	meta, ok := b.keyDir[key]
	if !ok {
		return ErrKeyNotExist, ""
	}
	// var reader *os.File

	datafile := strconv.Itoa(int(meta.fileID)) + ".dat"
	readerPath := b.name + "/" + datafile

	reader, err := os.Open(readerPath)
	if err != nil {
		return err, ""
	}

	offset := int64(meta.valuePos)

	buf := make([]byte, int(meta.valueSize))

	_, err = reader.ReadAt(buf, offset)
	if err != nil {
		return err, ""
	}

	return nil, string(buf)

}

func (b *bitcast) createActiveWriter(path string) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		log.Fatal("create activeWriter err:", err)
	}
	b.activeWriter = fd
}

func (b *bitcast) Put(key string, value string) error {

	keyBytes := []byte(key)
	valueBytes := []byte(value)

	datafile := strconv.Itoa(int(b.activeFid)) + ".dat"
	writerPath := b.name + "/" + datafile

	// activeWriter fd validate
	if b.activeWriter == nil {
		b.createActiveWriter(writerPath)
	}
	activeName := b.activeWriter.Name()
	if activeName != datafile {
		b.createActiveWriter(writerPath)
	}
	writerStat, err := b.activeWriter.Stat()
	if err != nil {
		log.Fatal("get writer stat error:", err)
	}
	offset := uint32(writerStat.Size())

	// k/v bytes content process
	header := &itemHeader{
		ts: uint32(time.Now().Unix()),
		// offset: uint32(writerStat.Size()),
		kSize: uint32(len(key)),
		vSize: uint32(len(value)),
	}
	headerBytes := header.toBytes()
	content := joinBytes(headerBytes, keyBytes, valueBytes)

	// content write
	_, err = b.activeWriter.Write(content)
	if err != nil {
		log.Fatal("write item error:")
	}

	// keyDir update
	meta := metainfo{
		fileID:    uint32(b.activeFid),
		valueSize: uint32(len(value)),
		valuePos:  offset + uint32(unsafe.Sizeof(itemHeader{})) + uint32(len(key)),
		ts:        header.ts,
	}
	b.keyDir[key] = meta

	return nil

}

func (b *bitcast) Close() {

}

func (b *bitcast) Delete(key string) error {
	return nil
}

func (b *bitcast) Merge() {

}

func (b *bitcast) EmptyInit() {
	b.activeFid = 0
	b.maxFid = 0
	b.keyDir = make(map[string]metainfo)
}

func (b *bitcast) LoadDatafile(filename string) {
	path := b.name + "/" + filename
	fid, _ := strconv.Atoi(filename[:len(filename)-3])
	fmt.Println("fid:", fid)
	fd, err := os.Open(path)
	if err != nil {
		log.Fatal("loadDatafile open error:", err)
	}

	headerSize := int64(unsafe.Sizeof(itemHeader{}))

	// headerBuf := make([]byte, headerSize)

	stat, err := fd.Stat()
	if err != nil {
		log.Fatal("loadDatafile file stat error:", err)
	}
	fileSize := int64(stat.Size())
	var offset int64 = 0
	for offset < fileSize {
		h, key, value := fetchItemOnce(fd, offset, true)
		fmt.Println("H:", h, "key,value:", key, value)

		meta := metainfo{
			fileID:    uint32(fid),
			valueSize: uint32(h.vSize),
			valuePos:  uint32(offset + headerSize + int64(len(key))),
			ts:        h.ts,
		}
		b.keyDir[key] = meta

		offset += headerSize + int64(h.kSize) + int64(h.vSize)
	}

}

func bytesToItemHeader(bytes []byte) *itemHeader {
	return (*itemHeader)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&bytes)).Data))

}

func fetchItemOnce(fd *os.File, offset int64, valueFlag bool) (h *itemHeader, key string, value string) {
	headerSize := int(unsafe.Sizeof(itemHeader{}))
	headerBuf := make([]byte, headerSize)

	_, err := fd.ReadAt(headerBuf, offset)
	if err != nil {
		log.Fatal("readat file  for metainfo error:", err)
	}

	header := bytesToItemHeader(headerBuf)
	keysize := int64(header.kSize)
	valuesize := int64(header.vSize)

	keybytes := make([]byte, int64(keysize))
	keyoffset := offset + int64(headerSize)
	_, err = fd.ReadAt(keybytes, keyoffset)
	if err != nil {
		log.Fatal("readat key error:", err)
	}

	if !valueFlag {
		return header, string(keybytes), ""
	}

	valuebytes := make([]byte, int64(valuesize))
	valueoffset := offset + int64(headerSize) + int64(keysize)
	_, err = fd.ReadAt(valuebytes, valueoffset)
	if err != nil {
		log.Fatal("readat value error:", err)
	}
	return header, string(keybytes), string(valuebytes)

}

// load all k/v items from persistent storage
func (b *bitcast) LoadData() {
	path := b.name

	//fetch all the datafile info
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal("read dir error ")
	}
	count := len(files)
	if 0 == count {
		b.EmptyInit()
		return
	}

	// load datafile to update bitcast.keyDir
	for i := 0; i < len(files); i++ {
		datafile := files[i]

		b.LoadDatafile(datafile.Name())

	}

	//TODO  process the max fid

	//TODO  set fid 0 to be the active fid and init activeWriter

	fmt.Println("KEYdIR:", b.keyDir)

}
