import request from '@/utils/request'

export function cronList(data) {
  return request({
    url: '/api/sql2cron/cron-list',
    method: 'post',
    data
  })
}

export function addCron(data) {
  return request({
    url: '/api/sql2cron/add-cron',
    method: 'post',
    data
  })
}

export function columnList(data) {
  return request({
    url: '/api/sql2cron/column-list',
    method: 'post',
    data
  })
}

export function examineLog(data) {
  return request({
    url: '/api/sql2cron/examine-log',
    method: 'post',
    data
  })
}

export function examine(data) {
  return request({
    url: '/api/sql2cron/examine',
    method: 'post',
    data
  })
}

export function deleteCron(data) {
  return request({
    url: '/api/sql2cron/delete-cron',
    method: 'post',
    data
  })
}
