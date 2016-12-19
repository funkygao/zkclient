package zookeeper

import (
	"io"
)

type FileHeader struct {
	Magic   int32
	Version int32
	Dbid    int64
}

func (fh *FileHeader) Deserialize(r io.Reader) error {
	return nil
}

type TxnHeader struct {
	ClientId int64
	Cxid     int32
	Zxid     int64
	Time     int64
	Type     int32
}

func (th *TxnHeader) Deserialize(r io.Reader) error {
	return nil
}
