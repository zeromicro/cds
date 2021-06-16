package ckgroup

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"github.com/dchest/siphash"
	"github.com/tal-tech/go-zero/core/logx"
	"golang.org/x/sync/errgroup"
)

var typeErr = errors.New("sliceData type must be []*sturct or []struct ")

func (g *dbGroup) InsertAuto(query string, hashTag string, sliceData interface{}) error {
	shardDatas, err := cutData2ShardData(sliceData, len(g.ShardNodes), hashTag)
	if err != nil {
		return err
	}

	err = g.opt.GroupInsertLimiter.Wait(context.Background())
	if err != nil {
		logx.Error(err)
	}

	var eg errgroup.Group
	for i, shardConn := range g.ShardNodes {

		shardData := shardDatas[i].Elem().Interface()
		innerShardConn := shardConn
		innerShardIndex := i + 1

		eg.Go(func() error {
			var err error
			for j := 1; j <= g.opt.RetryNum; j++ {
				err = innerShardConn.InsertAuto(query, shardData)
				if err == nil {
					return nil
				} else {
					logx.Errorf("[attempt %d/%d] shard[%d] all node exec failed. Last fail reason: %v, query: %s", j, g.opt.RetryNum, innerShardIndex, err, query)
				}
			}
			return err
		})
	}
	return eg.Wait()
}

type InsertErrDetail struct {
	Err        error
	ShardIndex int
	Datas      interface{}
}

func (g *dbGroup) InsertAutoDetail(query string, hashTag string, sliceData interface{}) ([]InsertErrDetail, error) {
	shardDatas, err := cutData2ShardData(sliceData, len(g.ShardNodes), hashTag)
	if err != nil {
		return nil, err
	}

	err = g.opt.GroupInsertLimiter.Wait(context.Background())
	if err != nil {
		logx.Error(err)
	}

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(g.ShardNodes))
	ch := make(chan InsertErrDetail, len(g.GetAllShard()))

	for i, shardConn := range g.ShardNodes {
		shardData := shardDatas[i].Elem().Interface()
		innerShardConn := shardConn
		innerShardIndex := i + 1

		go func() {
			defer waitGroup.Done()
			for j := 1; j <= g.opt.RetryNum; j++ {
				if err := innerShardConn.InsertAuto(query, shardData); err == nil {
					return
				} else {
					logx.Errorf("[attempt %d/%d] shard[%d] all node exec failed. Last fail reason: %v, query: %s", j, g.opt.RetryNum, innerShardIndex, err, query)
					if j == g.opt.RetryNum {
						ch <- InsertErrDetail{Err: err, ShardIndex: innerShardIndex, Datas: shardData}
					}
				}
			}
		}()
	}
	waitGroup.Wait()

	close(ch)
	var errDetail []InsertErrDetail
	for item := range ch {
		errDetail = append(errDetail, item)
	}
	sort.Slice(errDetail, func(i, j int) bool {
		return errDetail[i].ShardIndex < errDetail[j].ShardIndex
	})
	return errDetail, nil
}

func cutData2ShardData(sliceData interface{}, shardLen int, hashTag string) ([]reflect.Value, error) {
	outerType := reflect.TypeOf(sliceData)
	if outerType.Kind() != reflect.Slice {
		return nil, typeErr
	}
	sliceType := outerType.Elem()
	isPtr := false
	switch sliceType.Kind() {
	case reflect.Ptr:
		isPtr = true
		if sliceType.Elem().Kind() != reflect.Struct {
			return nil, typeErr
		}
	case reflect.Struct:
	default:
		return nil, typeErr
	}

	// 数组的元素是的类型 : *[]struct 或 *[]*struct
	shardDatas := make([]reflect.Value, 0, shardLen)
	for i := 0; i < shardLen; i++ {
		shardDatas = append(shardDatas, reflect.New(reflect.SliceOf(sliceType)))
	}

	sliceVal := reflect.ValueOf(sliceData)
	for i := 0; i < sliceVal.Len(); i++ {
		interVal := sliceVal.Index(i)
		var findTagVal reflect.Value
		if isPtr {
			findTagVal = interVal.Elem()
		} else {
			findTagVal = interVal
		}
		hashVal, err := findFieldValueByTag(findTagVal, DbTag, hashTag)
		if err != nil {
			return nil, err
		}
		shardIndex := siphash.Hash(0, 0, []byte(fmt.Sprint(hashVal))) % uint64(shardLen)

		shardDataSliceVal := shardDatas[shardIndex]
		shardDataSliceVal.Elem().Set(reflect.Append(shardDataSliceVal.Elem(), interVal))
		shardDatas[shardIndex] = shardDataSliceVal
	}
	return shardDatas, nil
}
