import { cronList, addCron, columnList, examineLog, examine, deleteCron } from '@/api/sqlcron'

const state = {
  name: '',
  avatar: '',
  introduction: '',
  roles: []
}

const mutations = {}

const actions = {
  cronList({ commit }, payload) {
    const { page, size } = payload
    return new Promise((resolve, reject) => {
      cronList({ page: page, size: size }).then(response => {
        const { data } = response
        for (let i = 0; i < data.list.length; i++) {
          switch (data.list[i].status) {
            case 0:
              data.list[i].status = 'Auditing'
              break
            case 1:
              data.list[i].status = 'Unsheduled'
              break
            case 2:
              data.list[i].status = 'Running'
              break
            case 3:
              data.list[i].status = 'Done'
              break
            case 4:
              data.list[i].status = 'Error'
              break
          }
        }
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  cronAdd({ commit }, payload) {
    const { model } = payload
    return new Promise((resolve, reject) => {
      addCron({ ...model }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  columnList({ commit }, payload) {
    const { content } = payload
    return new Promise((resolve, reject) => {
      columnList({ content: content }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  examineLog({ commit }, payload) {
    const { sqlCronId } = payload
    return new Promise((resolve, reject) => {
      examineLog({ sqlCronId: sqlCronId }).then(response => {
        const { data } = response
        for (var i = 0; i < data.list.length; i++) {
          if (data.list[i].isApprove === 1) {
            data.list[i].icon = 'el-icon-success'
          } else {
            data.list[i].icon = 'el-icon-error'
          }
        }
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  examineLog({ commit }, payload) {
    const { sqlCronId } = payload
    return new Promise((resolve, reject) => {
      examineLog({ sqlCronId: sqlCronId }).then(response => {
        const { data } = response
        for (var i = 0; i < data.list.length; i++) {
          if (data.list[i].isApprove === 1) {
            data.list[i].icon = 'el-icon-success'
          } else {
            data.list[i].icon = 'el-icon-error'
          }
        }
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  examine({ commit }, payload) {
    const { exmodel } = payload
    return new Promise((resolve, reject) => {
      examine({ ...exmodel }).then(response => {
        const { data } = response
        resolve(data)
      }).catch(error => {
        reject(error)
      })
    })
  },

  deleteCron({ commit }, payload) {
    const { id } = payload
    return new Promise((resolve, reject) => {
      deleteCron({ id: id }).then(response => {
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
