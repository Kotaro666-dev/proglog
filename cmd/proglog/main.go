package proglog

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github/Kotaro666-dev/prolog/internal/agent"
	"github/Kotaro666-dev/prolog/internal/config"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"
)

type cli struct {
	cfg cfg
}

type cfg struct {
	agent.Config
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
}

func main() {
	cli := &cli{}

	cmd := &cobra.Command{
		Use:     "proglo",
		PreRunE: cli.setupConfig,
		RunE:    cli.run,
	}

	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}

	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	cmd.Flags().String("config-file", "", "Path to config file")

	dataDir := path.Join(os.TempDir(), "proglog")
	cmd.Flags().String("data-dir", dataDir, "Directory to store log and Raft data.")
	cmd.Flags().String("node-name", hostname, "Unique server ID")

	cmd.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind Serf on.")
	cmd.Flags().Int("rpc-port", 8400, "Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")

	cmd.Flags().String("acl-model-file", "", "Path to ACL model.")
	cmd.Flags().String("acl-policy-file", "", "Path to ACL policy.")

	cmd.Flags().String("server-tls-cert-file", "", "Path to server tls cert.")
	cmd.Flags().String("server-tls-key-file", "", "Path to server tls key.")
	cmd.Flags().String("server-tls-ca-file", "", "Path to server certificate authority.")

	cmd.Flags().String("peer-tls-cert-file", "", "Path to peer tls cert.")
	cmd.Flags().String("peer-tls-key-file", "", "Path to peer tls key.")
	cmd.Flags().String("peer-tls-ca-file", "", "Path to peer certificate authority.")

	return viper.BindPFlags(cmd.Flags())
}

/// 設定を読み込んで、エージェントの準備をする
func (cli *cli) setupConfig(cmd *cobra.Command, args []string) error {
	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}
	viper.SetConfigFile(configFile)

	if err = viper.ReadInConfig(); err != nil {
		// 設定ファイルは、存在しなくても問題ない
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	cli.cfg.DataDir = viper.GetString("data-dir")
	cli.cfg.NodeName = viper.GetString("node-name")
	cli.cfg.BindAddress = viper.GetString("bind-addr")
	cli.cfg.RPCPort = viper.GetInt("rpc-port")
	cli.cfg.StartJoinAddress = viper.GetStringSlice("start-join-addrs")
	cli.cfg.Bootstrap = viper.GetBool("bootstrap")
	cli.cfg.ACLModelFile = viper.GetString("acl-mode-file")
	cli.cfg.ACLPolicyFile = viper.GetString("acl-policy-file")
	cli.cfg.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
	cli.cfg.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
	cli.cfg.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")
	cli.cfg.PeerTLSConfig.CertFile = viper.GetString("peer-tls-cert-file")
	cli.cfg.PeerTLSConfig.KeyFile = viper.GetString("peer-tls-key-file")
	cli.cfg.PeerTLSConfig.CAFile = viper.GetString("peer-tls-ca-file")

	if cli.cfg.ServerTLSConfig.CertFile != "" && cli.cfg.ServerTLSConfig.KeyFile != "" {
		cli.cfg.ServerTLSConfig.Server = true
		cli.cfg.Config.ServerTLSConfig, err = config.SetupTLSConfig(
			cli.cfg.ServerTLSConfig)
		if err != nil {
			return err
		}
	}

	if cli.cfg.PeerTLSConfig.CertFile != "" && cli.cfg.PeerTLSConfig.KeyFile != "" {
		cli.cfg.Config.PeerTLSConfig, err = config.SetupTLSConfig(cli.cfg.PeerTLSConfig)
		if err != nil {
			return err
		}
	}
	return err
}

func (cli *cli) run(cmd *cobra.Command, args []string) error {
	/// エージェントの作成
	agent, err := agent.New(cli.cfg.Config)
	if err != nil {
		return err
	}
	signalChannel := make(chan os.Signal, 1)
	/// OSからのシグナル処理
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	<-signalChannel
	/// OSがプログラムを終了させる場合、エージェントをグレースフルにシャットダウンする
	return agent.Shutdown()
}
