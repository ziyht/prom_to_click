package src

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type click struct {
	cfg    *ClickCfg
	db     *sql.DB
	dsn    string
	name   string
	health bool
	tag    string
	sigs   chan int8
}

func NewClick(name string, cfg *ClickCfg) (*click, error) {

	click := new(click)

	click.tag  = fmt.Sprintf("clickhouse[%s]", name)
	click.name = name
	click.cfg  = cfg
	click.sigs = make(chan int8, 16)

	click.init()

	return click, nil
}

func (c *click)init(){

	if c.cfg.Dsn == ""{
		mainHost      := "tcp://localhost:9000"
		username      := "?username=default"
		password      := ""
		database      := ""
		readTimeout   := ""
		writeTimeout  := ""
		altHosts      := ""

		if c.cfg.Server != ""{
			mainHost = "tcp://" + c.cfg.Server
		}
		if c.cfg.User != ""{
			username = "?username=" + c.cfg.User
		}
		if c.cfg.Passwd != ""{
			password = "&password=" + c.cfg.Passwd
		}
		if c.cfg.Database != ""{
			database = "&database=" + c.cfg.Database
		}
		if c.cfg.ReadTimeout > 0 {
			readTimeout = fmt.Sprintf("&read_timeout=%d", c.cfg.ReadTimeout)
		}
		if c.cfg.WriteTimeout > 0 {
			writeTimeout = fmt.Sprintf("&write_timeout=%d", c.cfg.WriteTimeout)
		}
		if len(c.cfg.AltHosts) > 0 {
			altHosts = "&alt_hosts=" + strings.Join(c.cfg.AltHosts, ",")
		}

		// eg: tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
		// see https://github.com/kshvakov/clickhouse
		c.dsn = mainHost + username + password + database + readTimeout + writeTimeout + altHosts
	} else {
		c.dsn = c.cfg.Dsn
	}
	slog.Infof("%s: discovered: %s", c.tag, c.dsn)

	go c.ConnectingRoutine()
	c.sigConnect()
}

func (c *click)sigConnect(){
	select {
		case c.sigs <- 1:
		default:					// no blocking set
	}
}

func (c *click)ConnectingRoutine(){

	for _ = range c.sigs{
		err := c.TryConnect()
		if err != nil {
			slog.Errorf("%s: connect failed: %s", c.tag, err)
			time.Sleep(time.Second)
		} else {
			slog.Infof("%s: connected ok", c.tag)
			for _ = range c.sigs{}			// clear channel
		}
	}
}

func (c *click)TryConnect() error {

	var err error

	if c.db == nil{
		c.db, err = sql.Open("clickhouse", c.dsn)
		if err != nil{
			return err
		}
	}

	err = c.db.Ping()
	if err != nil{
		c.health = false
	} else {
		c.health = true
	}

	return err
}

func (c *click)IsHealthy() bool {
	return c.health
}

func (c *click)Query(query string, args ...interface{}) (*sql.Rows, error) {
	if c.health == false{
		return nil, fmt.Errorf("status, unheathy")
		c.sigConnect()
	}

	return c.db.Query(query, args...)
}

type clicks struct {
	clicks map[string]*click
}

var ClicksMan *clicks

func (cs *clicks) init(){
	cs.clicks = map[string]*click{}

	for name, cfg := range Cfg.Servers{

		server, err := NewClick(name, &cfg)

		if err != nil{
			slog.Fatalf("create server '%s' faield: %s", name, err)
		}

		cs.clicks[name] = server
	}
}

func (cs *clicks) GetServer(name string) *click {
	click, exist := cs.clicks[name]

	if exist{
		return click
	} else {
		return nil
	}
}

func (cs *clicks) Query(name string, query string) (*sql.Rows, error) {

	click := cs.GetServer(name)

	if click != nil{
		return click.Query(query)
	} else {
		return nil, fmt.Errorf("server named '%s' can not be found")
	}
}

func initClickhouse() {

	ClicksMan = new(clicks)
	ClicksMan.init()
}
