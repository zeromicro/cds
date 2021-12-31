package canalx

import (
	"bytes"
	"encoding/json"
	"errors"
	"html/template"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/tal-tech/go-zero/core/logx"
)

type (
	Canalx struct {
		Addr     string
		UserName string
		Password string
		client   *http.Client
		token    map[string]string
	}
	LoginResponse struct {
		Code    int            `json:"code"`
		Message string         `json:"string"`
		Data    CanalTokenData `json:"data"`
	}
	CanalTokenData struct {
		Token string `json:"token"`
	}
	InstanceRequest struct {
		Addr      string
		User      string
		Password  string
		Dbname    string
		TableName string
		Topic     string
	}
	InstanceResponse struct {
		Code    int    `json:"code"`
		Message string `json:"string"`
		Data    string `json:"data"`
	}
	Instance struct {
		Id            int        `json:"id"`
		Name          string     `json:"name"`
		RunningStatus string     `json:"runningStatus"`
		ClusterID     string     `json:"clusterId"`
		UpdateTime    string     `json:"modifiedTime"`
		NodeServer    NodeServer `json:"nodeServer"`
	}
	NodeServer struct {
		Name string `json:"name"`
	}
	InstancesResponse struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    Data   `json:"data"`
	}
	InstancesDeleteResponse struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    string `json:"data"`
	}
	Data struct {
		Count  int        `json:"count"`
		Offset int        `json:"offset"`
		Page   int        `json:"page"`
		Size   int        `json:"size"`
		Items  []Instance `json:"items"`
	}
)

const (
	LOGIN_URL_SUFFIX = "/api/v1/user/login"
)

func NewCanalx(ip, host, username, password string) *Canalx {
	return &Canalx{
		Addr:     ip + ":" + host,
		UserName: username,
		Password: password,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *Canalx) login() error {
	userMap := map[string]string{
		"username": c.UserName,
		"password": c.Password,
	}
	respBytes, err := c.post("http://"+c.Addr+LOGIN_URL_SUFFIX, userMap, nil)
	if err != nil {
		logx.Error(err)
		return err
	}
	var result LoginResponse
	if err = json.Unmarshal(respBytes, &result); err != nil {
		logx.Error(err)
		return err
	}
	if result.Code != 20000 {
		return errors.New(result.Message)
	}
	c.token = map[string]string{
		"X-Token": result.Data.Token,
	}
	return nil
}

func (c *Canalx) AddInstances(config *mysql.Config, tableName, serverId string) error {
	if err := c.login(); err != nil {
		return err
	}
	tmpl, err := template.New("canal").Parse(canalxTmpl)
	if err != nil {
		logx.Error(err)
		return err
	}
	var writer bytes.Buffer
	topicName := "canal_" + config.DBName + "_" + tableName
	instance := InstanceRequest{
		Topic:     topicName,
		Password:  config.Passwd,
		User:      config.User,
		Addr:      config.Addr,
		Dbname:    config.DBName,
		TableName: tableName,
	}
	err = tmpl.Execute(&writer, instance)
	if err != nil {
		logx.Error(err)
		return err
	}
	instanceMap := map[string]string{
		"clusterServerId": "server:" + serverId,
		"content":         writer.String(),
		"name":            topicName,
	}
	respBytes, err := c.post("http://"+c.Addr+"/api/v1/canal/instance", instanceMap, c.token)
	if err != nil {
		logx.Error(err)
		return err
	}
	var res InstanceResponse
	err = json.Unmarshal(respBytes, &res)
	if err != nil {
		logx.Error(err)
		return err
	}
	if res.Code != 20000 {
		return errors.New(res.Message)
	}
	return nil
}

func (c *Canalx) DeleteInstance(id string) error {
	if err := c.login(); err != nil {
		logx.Error(err)
		return err
	}
	resp, err := c.delete("http://"+c.Addr+"/api/v1/canal/instance/"+id, c.token)
	if err != nil {
		logx.Error(err)
		return err
	}
	var ins InstancesDeleteResponse
	if err := json.Unmarshal(resp, &ins); err != nil {
		logx.Error(err)
		return err
	}
	if ins.Code != 20000 {
		logx.Error(err)
		return errors.New(ins.Message)
	}
	return nil
}

func (c *Canalx) GetInstanceByName(name string) (*Instance, error) {
	if err := c.login(); err != nil {
		return nil, err
	}
	params := map[string]string{
		"page": "1",
		"size": "1",
		"name": name,
	}
	respBytes, err := c.get("http://"+c.Addr+"/api/v1/canal/instances", params, c.token)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	var resp InstancesResponse
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 20000 {
		return nil, errors.New(resp.Message)
	}
	return &resp.Data.Items[0], nil
}

func (c *Canalx) GetInstancesList(page, size int) (*Data, error) {
	if err := c.login(); err != nil {
		return nil, err
	}
	params := map[string]string{
		"page": strconv.Itoa(page),
		"size": strconv.Itoa(size),
	}
	respBytes, err := c.get("http://"+c.Addr+"/api/v1/canal/instances", params, c.token)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	var resp InstancesResponse
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 20000 {
		return nil, errors.New(resp.Message)
	}
	return &resp.Data, nil
}

func (c *Canalx) post(url string, mp, header map[string]string) ([]byte, error) {
	data, err := json.Marshal(mp)
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	reader := bytes.NewReader(data)
	request, err := http.NewRequest("POST", url, reader)
	request.Header.Set("Content-Type", "application/json;charset=UTF-8")
	for key, value := range header {
		request.Header.Set(key, value)
	}
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	defer request.Body.Close()
	resp, err := c.client.Do(request)
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	return respBytes, nil
}

func (c *Canalx) get(url string, params, header map[string]string) ([]byte, error) {
	url += "?"
	for k, v := range params {
		url = url + k + "=" + v + "&"
	}
	reader := bytes.NewReader([]byte{})
	request, err := http.NewRequest("GET", url, reader)
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	for key, value := range header {
		request.Header.Set(key, value)
	}
	defer request.Body.Close()
	resp, err := c.client.Do(request)
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	return respBytes, nil
}

func (c *Canalx) delete(url string, header map[string]string) ([]byte, error) {
	reader := bytes.NewReader([]byte{})
	request, err := http.NewRequest("DELETE", url, reader)
	if err != nil {
		logx.Error(err)
		return nil, err
	}
	defer request.Body.Close()
	for key, value := range header {
		request.Header.Set(key, value)
	}
	resp, err := c.client.Do(request)
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logx.Error(err)
		return []byte{}, err
	}
	return respBytes, nil
}
