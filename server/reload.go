package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/tangrc99/MemTable/config"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/server/acl"
	"net"
	"os"
)

// reloadConfig 会重新读取配置文件中的字段，并根据更新后的配置进行参数更新。
func (s *Server) reloadConfig(fields []string) {

	logger.Debug("Receive config update notification")

	for i := range fields {
		switch fields[i] {
		case "Host", "Port":
			if config.Conf.Port == 0 {
				_ = s.listener.Close()
			} else {
				s.url = fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.Port)
			}

			l, err := net.Listen("tcp", s.url)
			if err != nil {
				logger.Errorf("Err change config %s", err.Error())
				continue
			}
			_ = s.listener.Close()
			s.listener = l
			logger.Infof("Listen url change to %s", s.url)
			go s.acceptLoop(s.listener)

		case "TLSPort", "AuthClient", "CertFile", "KeyFile", "CaCertFile":

			if config.Conf.TLSPort == 0 {
				_ = s.tlsListener.Close()
			} else {
				s.url = fmt.Sprintf("%s:%d", config.Conf.Host, config.Conf.TLSPort)
			}

			// 载入服务端证书和私钥
			srvCert, err := tls.LoadX509KeyPair(config.Conf.CertFile, config.Conf.KeyFile)
			if err != nil {
				logger.Panicf(err.Error())
			}

			// 载入根证书，用于客户端验证
			caCertPool := x509.NewCertPool()
			caCert, err := os.ReadFile(config.Conf.CaCertFile)
			if err != nil {
				logger.Panicf(err.Error())
			}
			if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
				logger.Panicf("Parse cert error, file: %s", config.Conf.CaCertFile)
			}
			tlsCfg := &tls.Config{
				InsecureSkipVerify: false,
				ClientAuth:         tls.RequestClientCert,
				ClientCAs:          caCertPool,
				Certificates:       []tls.Certificate{srvCert},
			}
			if config.Conf.AuthClient {
				tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
			}

			l, err := tls.Listen("tcp", s.tlsUrl, tlsCfg)
			if err != nil {
				logger.Errorf("Err change config %s", err.Error())
				continue
			}
			s.tlsListener = l
			logger.Infof("Listen tls-url change to %s", s.tlsUrl)
			go s.acceptLoop(s.tlsListener)

		case "LogDir", "LogLevel":
			err := logger.ChangeConfig(config.Conf.LogDir, "bin.log", logger.StringToLogLevel(config.Conf.Dir))
			if err != nil {
				logger.Errorf("Err change config %s", err.Error())
			}
		case "DataBases":
			logger.Error("Thermal renew 'databases' is not allowed")

		case "Timeout":
			s.cliTimeout = config.Conf.Timeout

		case "Daemonize":
			logger.Error("Thermal renew 'daemonize' is not allowed")

		case "Dir":
			logger.Error("Thermal renew 'dir' is not allowed")

		case "MaxClients":
			s.maxClients = config.Conf.MaxClients

		case "MaxMemory":
			// nothing to do

		case "AppendFsync":
			// nothing to do

		case "AppendOnly":
			if !s.aofEnabled {
				s.aof = newAOFBuffer(config.Conf.Dir + "appendonly.aof")
			}
			s.aofEnabled = config.Conf.AppendOnly

		case "GoPool", "GoPoolSize", "GoPoolSpawn":

		case "RDBFile":
			s.rdbFile = config.Conf.RDBFile
		case "ClusterEnable":
			logger.Error("Thermal renew 'cluster_enable' is not allowed")

		case "ClusterName":
			logger.Error("Thermal renew 'cluster_name' is not allowed")

		case "Eviction":
			logger.Error("Thermal renew 'eviction' is not allowed")

		case "SlowLogMaxLen":
			logger.Error("Thermal renew 'slowlog-max-len' is not allowed")

		case "SlowLogSlowerThan":

		case "ACLFile":
			s.acl = acl.NewAccessControlList(config.Conf.ACLFile)
		default:
			logger.Errorf("Thermal renew update unknown field %s", fields[i])
		}
	}
}
