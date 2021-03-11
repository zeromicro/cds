import request from '@/utils/request'

export function login(data) {
  return request({
    url: '/api/galaxy/user/login',
    method: 'post',
    data
  })
}

// dm
export function databaseList(data) {
  return request({
    url: '/api/galaxy/html/database-list',
    method: 'post',
    data
  })
}

export function dmList(data) {
  return request({
    url: '/api/galaxy/html/dm-list',
    method: 'post',
    data
  })
}

export function dmStop(data) {
  return request({
    url: '/api/galaxy/html/dm-stop',
    method: 'post',
    data
  })
}

export function dmAdd(data) {
  return request({
    url: '/api/galaxy/html/dm-add',
    method: 'post',
    data
  })
}

export function dmDelete(data) {
  return request({
    url: '/api/galaxy/html/dm-delete',
    method: 'post',
    data
  })
}

export function dmRedo(data) {
  return request({
    url: '/api/galaxy/html/dm-redo',
    method: 'post',
    data
  })
}

export function defaultConfig() {
  return request({
    url: '/api/galaxy/html/default-config',
    method: 'get'
  })
}

export function listDatabases() {
  return request({
    url: '/api/galaxy/html/list-databases',
    method: 'post'
  })
}

export function generateSql(data) {
  return request({
    url: '/api/galaxy/html/generate-create-sql',
    method: 'post',
    data
  })
}

export function listTables(data) {
  return request({
    url: '/api/galaxy/html/list-tables',
    method: 'post',
    data
  })
}

export function execSql(data) {
  return request({
    url: '/api/galaxy/html/exec-sql',
    method: 'post',
    data
  })
}

// connector

export function connectorList(data) {
  return request({
    url: '/api/galaxy/html/connector-list',
    method: 'post',
    data
  })
}

export function connectorAdd(data) {
  return request({
    url: '/api/galaxy/html/connector-add',
    method: 'post',
    data
  })
}

export function connectorDelete(data) {
  return request({
    url: '/api/galaxy/html/connector-delete',
    method: 'post',
    data
  })
}

// rtu

export function rtuList(data) {
  return request({
    url: '/api/galaxy/html/rtu-list',
    method: 'post',
    data
  })
}

export function rtuAdd(data) {
  return request({
    url: '/api/galaxy/html/rtu-add',
    method: 'post',
    data
  })
}

export function rtuRedo(data) {
  return request({
    url: '/api/galaxy/html/rtu-redo',
    method: 'post',
    data
  })
}

export function rtuStop(data) {
  return request({
    url: '/api/galaxy/html/rtu-stop',
    method: 'post',
    data
  })
}

export function rtuDelete(data) {
  return request({
    url: '/api/galaxy/html/rtu-delete',
    method: 'post',
    data
  })
}
