package zkclient

// Marshaller is the interface that wraps a record that can znode value.
type Marshaller interface {
	Marshal() []byte
}
