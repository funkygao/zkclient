package zkclient

type Record interface {
	Marshal() []byte
}
