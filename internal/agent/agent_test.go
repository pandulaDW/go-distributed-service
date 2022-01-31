package agent

import (
	"github.com/pandulaDW/go-distributed-service/internal/config"
	"testing"
)

func TestAgent(t *testing.T) {
	config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        true,
	})
}
