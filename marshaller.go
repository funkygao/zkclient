package zkclient

type Marshaller interface {
	Marshal() []byte
}
