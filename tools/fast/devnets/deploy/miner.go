package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"

	"github.com/filecoin-project/go-filecoin/address"
	"github.com/filecoin-project/go-filecoin/tools/fast"
	"github.com/filecoin-project/go-filecoin/tools/fast/series"
	lpfc "github.com/filecoin-project/go-filecoin/tools/iptb-plugins/filecoin/local"
	"github.com/filecoin-project/go-filecoin/types"
)

type MinerConfig struct {
	CommonConfig
	FaucetURL        string
	AutoSealInterval int
	Collateral       int
	AskPrice         string
	AskExpiry        int
	SectorSize       string
}

type MinerProfile struct {
	config MinerConfig
	runner FASTRunner
}

func NewMinerProfile(configfile string) (Profile, error) {
	cf, err := os.Open(configfile)
	if err != nil {
		return nil, errors.Wrapf(err, "config file %s", configfile)
	}

	defer cf.Close()

	dec := json.NewDecoder(cf)

	var config MinerConfig
	if err := dec.Decode(&config); err != nil {
		return nil, errors.Wrap(err, "config")
	}

	blocktime, err := time.ParseDuration(config.BlockTime)
	if err != nil {
		return nil, err
	}

	runner := FASTRunner{
		WorkingDir: config.WorkingDir,
		ProcessArgs: fast.FilecoinOpts{
			InitOpts: []fast.ProcessInitOption{
				fast.POGenesisFile(config.GenesisCarFile),
				NetworkPO(config.Network),
				fast.POPeerKeyFile(config.PeerkeyFile), // Needs to be last
			},
			DaemonOpts: []fast.ProcessDaemonOption{
				fast.POBlockTime(blocktime),
			},
		},
		PluginOptions: map[string]string{
			lpfc.AttrLogJSON:  config.LogJSON,
			lpfc.AttrLogLevel: config.LogLevel,
		},
	}

	return &MinerProfile{config, runner}, nil
}

func (p *MinerProfile) Pre() error {
	ctx := context.Background()

	node, err := GetNode(ctx, lpfc.PluginName, p.runner.WorkingDir, p.runner.PluginOptions, p.runner.ProcessArgs)
	if err != nil {
		return err
	}

	if _, err := os.Stat(p.runner.WorkingDir + "/repo"); os.IsNotExist(err) {
		if o, err := node.InitDaemon(ctx); err != nil {
			io.Copy(os.Stdout, o.Stdout())
			io.Copy(os.Stdout, o.Stderr())
			return err
		}
	} else if err != nil {
		return err
	}

	cfg, err := node.Config()
	if err != nil {
		return err
	}

	cfg.Observability.Metrics.PrometheusEnabled = true

	// IPTB changes this to loopback and a random port
	cfg.Swarm.Address = "/ip4/0.0.0.0/tcp/6000"

	if err := node.WriteConfig(cfg); err != nil {
		return err
	}

	return nil
}

func (p *MinerProfile) Daemon() error {
	args := []string{}
	for _, argfn := range p.runner.ProcessArgs.DaemonOpts {
		args = append(args, argfn()...)
	}

	fmt.Println(strings.Join(args, " "))

	return nil
}

func (p *MinerProfile) Post() error {
	ctx := context.Background()
	miner, err := GetNode(ctx, lpfc.PluginName, p.runner.WorkingDir, p.runner.PluginOptions, p.runner.ProcessArgs)
	if err != nil {
		return err
	}

	ctxWaitForAPI, cancel := context.WithTimeout(ctx, 10*time.Minute)
	if err := WaitForAPI(ctxWaitForAPI, miner); err != nil {
		return err
	}
	cancel()

	defer miner.DumpLastOutput(os.Stdout)

	var minerAddress address.Address
	if err := miner.ConfigGet(ctx, "mining.minerAddress", &minerAddress); err != nil {
		return err
	}

	// If the miner address is set then we are restarting
	if minerAddress == address.Undef {
		err := WaitForChainSync(ctx, miner)
		if err != nil {
			return err
		}
		if err := FaucetRequest(ctx, miner, p.config.FaucetURL); err != nil {
			return err
		}

		collateral := big.NewInt(int64(p.config.Collateral))
		price, _, err := big.ParseFloat(p.config.AskPrice, 10, 128, big.AwayFromZero)
		if err != nil {
			return err
		}

		expiry := big.NewInt(int64(p.config.AskExpiry))

		sectorSize, ok := types.NewBytesAmountFromString(p.config.SectorSize, 10)
		if !ok {
			return fmt.Errorf("Failed to parse sector size %s", p.config.SectorSize)
		}

		_, err = series.CreateStorageMinerWithAsk(ctx, miner, collateral, price, expiry, sectorSize)
		if err != nil {
			return err
		}

		if err := miner.MiningStart(ctx); err != nil {
			return err
		}
	} else {
		if err := miner.MiningStart(ctx); err != nil {
			return err
		}
	}
	return nil
}

// WaitForAPI will poll the ID command eveyr minutes and wait for it to return without error
// or until the context is done. An error is only returned if the context returns an error.
func WaitForAPI(ctx context.Context, p *fast.Filecoin) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Minute):
			if _, err := p.ID(ctx); err != nil {
				continue
			}

			return nil
		}
	}
}

func WaitForChainSync(ctx context.Context, node *fast.Filecoin) error {
	fmt.Println("Waiting for chain to sync")
	for {
		chainStatus, err := node.ChainStatus(ctx)
		if err != nil {
			return err
		}
		if chainStatus.SyncingComplete && chainStatus.SyncingHeight > 0 {
			fmt.Println("Chain Syncing Complete")
			break
		}
		time.Sleep(5 * time.Second)
	}
	return nil
}

func FaucetRequest(ctx context.Context, p *fast.Filecoin, uri string) error {
	var toAddr address.Address
	if err := p.ConfigGet(ctx, "wallet.defaultAddress", &toAddr); err != nil {
		return err
	}

	data := url.Values{}
	data.Set("target", toAddr.String())

	var msgcid string
	for {
		resp, err := http.PostForm(uri, data)
		if err != nil {
			return err
		}
		//statusCode := resp.StatusCode
		if resp.StatusCode == http.StatusOK {
			msgcid = resp.Header.Get("Message-Cid")
			break
		}

		timeout := 300 * time.Second
		fmt.Println("FaucetRequest failed. Trying again in", timeout)
		time.Sleep(timeout)
	}

	fmt.Println("msgcid", msgcid)
	mcid, err := cid.Decode(msgcid)
	if err != nil {
		return fmt.Errorf("Failed to decode %s: %s", msgcid, mcid)
	}

	if _, err := p.MessageWait(ctx, mcid); err != nil {
		return err
	}

	return nil
}

func (p *MinerProfile) Main() error { return nil }
