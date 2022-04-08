package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"strings"
	"sync"
)

const (
	url = `https://eth.getblock.io/mainnet/`

	requestBodyFormat      = `{"id": "getblock.io", "jsonrpc": "2.0", "method": "%s", "params": [%s]}`
	methodBlockNumber      = `eth_blockNumber`
	methodGetBlockByNumber = `eth_getBlockByNumber`
	methodGetBlockByHash   = `eth_getBlockByHash`

	headerAPIKey = `x-api-key`
	valueAPIKey  = `819fa71f-471a-4136-a863-1aacb6ea2ce4`

	headerContentType = `content-Type`
	valueContentType  = `application/json`
)

var (
	min = big.NewInt(0)
)

type LastBlock struct {
	Number string `json:"result"`
}

type Block struct {
	*BlockContent `json:"result"`
}

type BlockContent struct {
	Number       string         `json:"number"`
	Hash         string         `json:"mixHash"`
	PrevHash     string         `json:"parentHash"`
	Transactions []*Transaction `json:"transactions"`
}

type Transaction struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Value string `json:"value"`
}

func main() {
	// get last block number
	body, err := postRequest(methodBlockNumber, ``)
	if err != nil {
		panic(err)
	}

	var lastBlockNum *LastBlock
	err = json.Unmarshal(body, &lastBlockNum)
	if err != nil {
		panic(err)
	}

	// get the whole block data by its number
	body, err = postRequest(methodGetBlockByNumber, fmt.Sprintf(`"%s", true`, lastBlockNum.Number))
	if err != nil {
		panic(err)
	}

	block, err := unmarshalToBlock(body)
	if err != nil {
		panic(err)
	}

	length := 100

	// key is wallet number, values is absolute balance change
	wallets := make(map[string]*big.Int)
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	wg.Add(1)
	go checkTransactions(&wallets, block.Transactions, wg, mu)

	hashes := make(chan string, length-1)
	bodies := make(chan []byte, length-1)

	go getBlockBodyByHash(hashes, bodies)
	hashes <- block.PrevHash

	for i := 0; i < length-1; i++ {
		block, err = unmarshalToBlock(<-bodies)
		if err != nil {
			panic(err)
		}

		hashes <- block.PrevHash

		wg.Add(1)
		go checkTransactions(&wallets, block.Transactions, wg, mu)
	}

	wg.Wait()

	close(hashes)
	close(bodies)

	// find max change between all wallets
	wallet := ""
	change := min
	for key, val := range wallets {
		if val.Cmp(change) == 1 {
			change = val
			wallet = key
		}
	}

	fmt.Printf(`The balance of wallet "%s" has changed the most in absolute value - '%s'`, wallet, change)
}

func getBlockBodyByHash(hashes <-chan string, bodies chan<- []byte) {
	var (
		body []byte
		err  error
	)

	for hash := range hashes {
		// sometimes it's error that upstream connect error or disconnect/reset before headers, that's why needs retries
		for i := 0; i < 3; i++ {
			body, err = postRequest(methodGetBlockByHash, fmt.Sprintf(`"%s", true`, hash))
			if err == nil {
				break
			}
		}

		if err != nil {
			panic(err)
		}

		bodies <- body
	}
}

func checkTransactions(wallets *map[string]*big.Int, txs []*Transaction, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()

	for _, tx := range txs {
		val := new(big.Int)
		val.SetString(tx.Value, 0)
		if val.Cmp(min) == 0 {
			return
		}

		mu.Lock()
		if _, ok := (*wallets)[tx.From]; !ok {
			(*wallets)[tx.From] = val
		} else {
			(*wallets)[tx.From].Add((*wallets)[tx.From], val)
		}

		if _, ok := (*wallets)[tx.To]; !ok {
			(*wallets)[tx.To] = val
		} else {
			(*wallets)[tx.To].Add((*wallets)[tx.To], val)
		}
		mu.Unlock()
	}
}

func unmarshalToBlock(body []byte) (*Block, error) {
	var block *Block
	err := json.Unmarshal(body, &block)
	if err != nil {
		return nil, err
	}

	return block, err
}

func postRequest(method, params string) ([]byte, error) {
	reqBody := strings.NewReader(fmt.Sprintf(requestBodyFormat, method, params))

	req, err := http.NewRequest(http.MethodPost, url, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Add(headerAPIKey, valueAPIKey)
	req.Header.Add(headerContentType, valueContentType)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if strings.Contains(string(body), `error`) {
		return nil, fmt.Errorf("response has error: %s", body)
	}

	return body, nil
}
