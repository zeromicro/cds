package ckgroup

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/dchest/siphash"
	"github.com/tal-tech/go-zero/core/logx"
	"golang.org/x/sync/errgroup"
)

func (g *dbGroup) InsertAuto(query string, hashTag string, sliceData interface{}) error {
	typeErr := errors.New("sliceData type must be []*sturct or []struct ")

	outerType := reflect.TypeOf(sliceData)
	if outerType.Kind() != reflect.Slice {
		return typeErr
	}
	sliceType := outerType.Elem()
	isPtr := false
	switch sliceType.Kind() {
	case reflect.Ptr:
		isPtr = true
		if sliceType.Elem().Kind() != reflect.Struct {
			return typeErr
		}
	case reflect.Struct:
	default:
		return typeErr
	}

	//数组的元素是的类型 : *[]struct 或 *[]*struct
	shardDatas := make([]reflect.Value, 0, len(g.ShardNodes))
	for range g.ShardNodes {
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
		hashVal, err := fieldByTag(findTagVal, DbTag, hashTag)
		if err != nil {
			return err
		}
		shardIndex := siphash.Hash(0, 0, []byte(fmt.Sprint(hashVal))) % uint64(len(g.ShardNodes))

		shardDataSliceVal := shardDatas[shardIndex]
		shardDataSliceVal.Elem().Set(reflect.Append(shardDataSliceVal.Elem(), interVal))
		shardDatas[shardIndex] = shardDataSliceVal
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
				if err != nil {
					logx.Errorf(
						"[attempt %d/%d] shard[%d] all node exec failed. Last fail reason: %v, query: %s",
						j, g.opt.RetryNum, innerShardIndex, err, query)
				}
			}
			return err
		})
	}
	return eg.Wait()
}
