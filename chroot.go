package zkclient

import (
	"path"
	"strings"
)

func parseZkConnStr(zkConnStr string) (servers []string, chroot string, err error) {
	firstSlashOffset := strings.Index(zkConnStr, "/")
	if firstSlashOffset == -1 {
		// no chroot
		servers = strings.Split(zkConnStr, ",")
		return
	}

	chrootPath := zkConnStr[firstSlashOffset:]
	if len(chrootPath) > 1 {
		chroot = path.Clean(strings.TrimSpace(chrootPath))
		// validate path and return err TODO
	}
	servers = strings.Split(zkConnStr[:firstSlashOffset], ",")

	return
}
