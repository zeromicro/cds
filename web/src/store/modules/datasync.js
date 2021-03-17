import { login, databaseList, dmList, dmRedo, dmStop, dmDelete, defaultConfig, listDatabases, generateSql, listTables, execSql, dmAdd, connectorList, connectorAdd, connectorDelete, rtuList, rtuAdd, rtuRedo, rtuStop, rtuDelete } from '@/api/datasync'
import { getToken, setToken, removeToken } from '@/utils/auth'

const state = {
  name: '',
  avatar: '',
  introduction: '',
  roles: []
}

const mutations = {}

const actions = {

  login({ commit }, payload) {
    const { email, password } = payload
    return new Promise((resolve, reject) => {
      login({ email: email, password: password }).then(response => {
        const { data } = response
        setToken(data.auth)
        window.location.href = '/#/datasync'
      }).catch(error => {
        reject(error)
      })
    })
  },

  // dm s
  databaseList({ commit }, payload) {
    const { service } = payload
    return new Promise((resolve, reject) => {
      databaseList({ string: service }).then(response => {
        const { data } = response
        resolve(data.stringList)
      }).catch(error => {
        reject(error)
      })
    })
  },

  dmList({ commit }, payload) {
    const { page, size, dbName } = payload
    return new Promise((resolve, reject) => {
      dmList({ page: page, size: size, dbName: dbName }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  dmStop({ commit }, payload) {
    const { id } = payload
    return new Promise((resolve, reject) => {
      dmStop({ string: id }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  dmDelete({ commit }, payload) {
    const { id } = payload
    return new Promise((resolve, reject) => {
      dmDelete({ string: id }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  dmAdd({ commit }, payload) {
    const { dmModel, windowStartHour, windowEndHour } = payload
    return new Promise((resolve, reject) => {
      dmAdd({ ...dmModel, windowStartHour: windowStartHour.toString(), windowEndHour: windowEndHour.toString() }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  dmRedo({ commit }, payload) {
    const { id } = payload
    return new Promise((resolve, reject) => {
      dmRedo({ string: id }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  defaultConfig({ commit }) {
    return new Promise((resolve, reject) => {
      defaultConfig({}).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  listDatabases({ commit }) {
    return new Promise((resolve, reject) => {
      listDatabases({}).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  generateSql({ commit }, payload) {
    const { dsn, table, database } = payload
    return new Promise((resolve, reject) => {
      generateSql({ dsn: dsn, table: table, database: database }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  listTables({ commit }, payload) {
    const { string } = payload
    return new Promise((resolve, reject) => {
      listTables({ string: string }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  execSql({ commit }, payload) {
    const { sql } = payload
    return new Promise((resolve, reject) => {
      execSql({ sql: sql }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  // connector s
  connectorList({ commit }, payload) {
    const { page, size, dbName } = payload
    return new Promise((resolve, reject) => {
      connectorList({ page: page, size: size, dbName: dbName }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  connectorAdd({ commit }, payload) {
    const { model } = payload
    return new Promise((resolve, reject) => {
      connectorAdd({ ...model }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  connectorDelete({ commit }, payload) {
    const { sourceId, type } = payload
    return new Promise((resolve, reject) => {
      connectorDelete({ sourceId: sourceId, type: type }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  // rtu s
  rtuList({ commit }, payload) {
    const { page, size, dbName } = payload
    return new Promise((resolve, reject) => {
      rtuList({ page: page, size: size, dbName: dbName }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  rtuAdd({ commit }, payload) {
    const { model } = payload
    return new Promise((resolve, reject) => {
      rtuAdd({ ...model }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  rtuRedo({ commit }, payload) {
    const { string } = payload
    return new Promise((resolve, reject) => {
      rtuRedo({ string: string }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  rtuStop({ commit }, payload) {
    const { string } = payload
    return new Promise((resolve, reject) => {
      rtuStop({ string: string }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  rtuDelete({ commit }, payload) {
    const { string } = payload
    return new Promise((resolve, reject) => {
      rtuDelete({ string: string }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions
}
