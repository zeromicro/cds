package module

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/tal-tech/cds/cmd/dm/cmd/sync/config"
	"github.com/tal-tech/go-zero/core/logx"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Manager struct {
	Controllers  map[string]*chan bool
	Tasks        *TaskQueue
	Config       *config.Config
	CountManager *chan int
	StatusHelper *StatusHelper
}

const (
	layout        = "2006-01-02 15:04:05"
	StopKeyPrefix = "/hera/dm/stop-job/"
)

func NewManager(controllers map[string]*chan bool, tasks *TaskQueue, config *config.Config, countManager *chan int, statusHelper *StatusHelper) *Manager {
	return &Manager{
		Controllers:  controllers,
		Tasks:        tasks,
		Config:       config,
		CountManager: countManager,
		StatusHelper: statusHelper,
	}
}

func (m *Manager) AddController(taskID string) *chan bool {
	controller := make(chan bool)
	m.Controllers[taskID] = &controller
	return &controller
}

func (m *Manager) HandleJob(job *config.Job, mutex *concurrency.Mutex) error {
	IncCountOfTask()
	status, err := m.StatusHelper.ReadStatus(job.ID)
	if err != nil {
		return err
	}
	idType := ""
	if status.ID != "" {
		switch status.Status {
		case config.STATUS_PAUSE:
			idType = status.Information
		case config.STATUS_RUNNING:
			if time.Now().Unix()-status.UpdateTime.Unix() <= 3600 {
				return nil
			}
		default:
			return nil
		}
	}
	stopChan := make(chan bool)
	service := NewService(job, m.Config, m.AddController(job.ID), idType, mutex, &stopChan)
	m.Tasks.Put(service)
	err = m.StatusHelper.WriteStatus(job.ID, config.STATUS_PENDING, fmt.Sprintf(TipStatusPending, job.WindowPeriod.StartHour, job.WindowPeriod.EndHour))
	return err
}

func (m *Manager) Consume() {
	maxCount := m.Config.MaxParallelJobCount
	defer close(*m.CountManager)
	for {
		if !m.Tasks.Empty() {
			if len(*m.CountManager) >= maxCount {
				logx.Info(strconv.Itoa(len(*m.CountManager)) + "/" + strconv.Itoa(maxCount) + " Tasks are Running")
			} else if elem, ok := m.Tasks.Take(); ok {
				go func() {
					defer func() {
						if err := recover(); err != nil {
							logx.Error(err)
							logx.Error(string(debug.Stack()))
						}
					}()
					*m.CountManager <- 1
					if err := m.executeTask(elem); err != nil {
						logx.Error(err)
					}
					*elem.StopChan <- true
				}()
				go func() {
					if err := m.StopJob(elem); err != nil {
						logx.Error(err)
					}
				}()
			}
		} else {
			time.Sleep(5 * time.Second)
		}
	}
}

func (m *Manager) executeTask(service *Service) error {
	if err := m.StatusHelper.WriteStatus(service.Job.ID, config.STATUS_RUNNING, TipStatusRunning); err != nil {
		return err
	}
	if err := service.Mutex.Unlock(context.TODO()); err != nil {
		logx.Error(err)
	}
	var err error
	if firstID, e := service.Run(); e != nil {
		err = m.StatusHelper.WriteStatus(service.Job.ID, config.STATUS_ERROR, fmt.Sprintf(TipStatusError, e.Error()))
	} else if firstID == "stopped" {
		err = m.StatusHelper.WriteStatus(service.Job.ID, config.STATUS_STOPPED, fmt.Sprint(TipStatusStoppedRunning))
	} else if len(firstID) != 0 {
		err = m.StatusHelper.WriteStatus(service.Job.ID, config.STATUS_PAUSE, firstID)
		m.Tasks.Put(service)
	} else {
		err = m.StatusHelper.WriteStatus(service.Job.ID, config.STATUS_FINISHED, fmt.Sprintf(TipStatusFinished, time.Now().Format(layout)))
	}
	if err != nil {
		logx.Error(err)
	}
	<-*m.CountManager
	return err
}

func (m *Manager) StopJob(service *Service) error {
	ticker := time.NewTicker(time.Second * 5)
	IncCountOfRunningGoRoutine()
	for {
		<-ticker.C
		select {
		case <-*service.StopChan:
			DecCountOfRunningGoRoutine()
			break
		default:
			status, err := m.StatusHelper.ReadStatus(service.Job.ID)
			if err != nil {
				return err
			}
			resp, err := m.StatusHelper.Client.Get(context.TODO(), StopKeyPrefix+service.Job.ID)
			if err != nil {
				return err
			}
			if resp.Count == 0 {
				// if not get the stop cmd , flush the running status update time
				if status.ID != "" && status.Status == config.STATUS_RUNNING {
					err := m.StatusHelper.WriteStatus(service.Job.ID, config.STATUS_RUNNING, TipStatusRunning)
					if err != nil {
						return err
					}
				}
				continue
			}
			if err != nil {
				return err
			}
			switch status.Status {
			case config.STATUS_RUNNING:
				*m.Controllers[service.Job.ID] <- true
			case config.STATUS_PENDING:
				if !m.Tasks.RemoveByID(service.Job.ID) {
					err = errors.New(StatusPendingError)
				} else {
					err = m.StatusHelper.WriteStatus(service.Job.ID, config.STATUS_STOPPED, fmt.Sprint(TipStatusStoppedPending))
				}
			default:
				err = fmt.Errorf(StatusCannotStopError, status.Status)
			}

			return err
		}
	}
}
