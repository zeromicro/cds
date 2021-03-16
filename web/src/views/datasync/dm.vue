<template>
  <div class="app-container">
    <div class="filter-container">
      <label class="radio-label">Choose Database: </label>
      <el-select
        v-model="database"
        style="margin-bottom: 20px"
        @change="setDbAndReflesh()"
      >
        <el-option
          v-for="item in options"
          :key="item"
          :label="item"
          :value="item"
        />
      </el-select>
      <el-tooltip effect="light" placement="bottom" content="refresh">
        <el-button
          type="text"
          class="el-icon-refresh"
          style="float: right; width: 30px; font-size: 20px"
          @click="refresh(pager.page)"
        />
      </el-tooltip>
      <el-tooltip effect="light" placement="bottom" content="add">
        <el-button
          type="text"
          class="el-icon-plus"
          style="float: right; width: 30px; font-size: 20px"
          @click="loadModel()"
        />
      </el-tooltip>
    </div>
    <el-table
      v-loading="listLoading"
      :data="list"
      border
      fit
      highlight-current-row
      style="width: 100%"
    >
      <el-table-column label="Job Name" sortable prop="name" align="center">
        <template slot-scope="{ row }">
          <span>{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column label="Status" sortable prop="status" align="center">
        <template slot-scope="{ row }">
          <span>{{ row.status }}</span>
        </template>
      </el-table-column>
      <el-table-column label="Info" prop="information" align="center">
        <template slot-scope="{ row }">
          <span>{{ row.information }}</span>
        </template>
      </el-table-column>
      <el-table-column label="Run Betwenn" align="center">
        <template slot-scope="{ row }">
          <span v-if="row.windowStartHour > 0 && row.windowEndHour > 0">
            {{ row.windowStartHour }}:00-{{ row.windowEndHour }}:00
          </span>
        </template>
      </el-table-column>
      <el-table-column
        label="Last Update Time"
        sortable
        prop="updateTime"
        align="center"
      >
        <template slot-scope="{ row }">
          <span>{{ row.updateTime }}</span>
        </template>
      </el-table-column>
      <el-table-column label="Operation" align="center">
        <template slot-scope="{ row }">
          <el-tooltip effect="light" placement="bottom" content="redo">
            <el-button
              v-if="row.status !== 'running'"
              type="text"
              class="el-icon-refresh-right"
              @click="redo(row.id)"
            />
          </el-tooltip>
          <el-tooltip effect="light" placement="bottom" content="stop">
            <el-button
              v-if="row.status === 'running'"
              type="text"
              class="el-icon-video-pause"
              @click="stop(row.id)"
            />
          </el-tooltip>
          <el-tooltip effect="light" placement="bottom" content="delete">
            <el-button
              type="text"
              class="el-icon-delete"
              @click="del(row.id)"
            />
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
    <div class="pagination-container">
      <el-pagination
        :current-page.sync="pager.page"
        :page-size="10"
        layout="prev, pager, next"
        :total="pager.total"
        @current-change="refresh"
      />
    </div>
    <el-dialog
      title="Add Full Sync Task"
      :close-on-click-modal="false"
      :visible.sync="dialogVisible"
      top="1cm"
    >
      <el-tabs v-model="defaultTab">
        <el-tab-pane label="Data Source Info" name="dmAddSource">
          <el-form label-position="middle">
            <el-form-item label="Connection String">
              <el-input v-model="model.source.dsn">
                <el-button
                  slot="append"
                  :disabled="!model.source.dsn"
                  @click="listTables"
                >Connect</el-button>
              </el-input>
            </el-form-item>
            <el-form-item label="Choose Table">
              <el-select
                v-model="model.source.selectedTable"
                filterable
                multiple
                style="width: 100%"
                placeholder="Select"
              >
                <el-option
                  v-for="table in model.source.tables"
                  :key="table"
                  :label="table"
                  :value="table"
                />
              </el-select>
            </el-form-item>
            <el-form-item label="Suffix">
              <el-input v-model="model.source.suffix" />
            </el-form-item>
            <el-form-item label="Run Between">
              <el-input-number
                v-model="model.windowStartHour"
                :controls="false"
                :min="0"
                :max="23"
              />
              —
              <el-input-number
                v-model="model.windowEndHour"
                :controls="false"
                :min="0"
                :max="23"
              />
            </el-form-item>
            <!-- <el-form-item>
              <el-button
                :disabled="
                  model.source.dsn == '' ||
                    model.source.selectedTable.length == 0 ||
                    model.target.selectedDatabase == ''
                "
                @click="generateSql()"
              >
                Generate Create Table SQL
              </el-button>
              <el-button
                :disabled="!model.source.createTableSql.length > 0"
                @click="execSql()"
              >
                Send SQL To Clickhouse
              </el-button>
            </el-form-item> -->
            <!-- <el-input
              v-for="(item, index) in model.source.createTableSql"
              :key="index"
              type="textarea"
              :value="item"
              :rows="10"
            /> -->
          </el-form>
        </el-tab-pane>
        <el-tab-pane label="Target Clickhouse Database Info" name="dmAddTarget">
          <el-form label-position="top">
            <el-form-item>
              <el-input
                v-model="model.target.queryNode"
                class="shards-input"
                readonly
              >
                <div slot="prepend" style="width: 100px">Query Node</div>
              </el-input>
            </el-form-item>
            <el-form-item label="Clickhouse Shards">
              <div
                v-for="(shard, i) in model.target.shards"
                :key="i"
                style="margin-bottom: 10px"
              >
                <el-input
                  v-for="(item, index) in shard"
                  :key="index"
                  class="shards-input"
                  style="margin-bottom: 5px"
                  :value="item"
                  readonly
                >
                  <div slot="prepend" style="width: 100px">
                    {{ index === 0 ? `Insert Node` : `Backup Node` }}
                  </div>
                </el-input>
              </div>
            </el-form-item>
            <el-form-item label="Cluster Name">
              <el-input v-model="model.target.cluster" readonly />
            </el-form-item>
            <el-form-item label="Choose Database">
              <el-select
                v-model="model.target.selectedDatabase"
                filterable
                placeholder="Select"
              >
                <el-option
                  v-for="db in model.target.databases"
                  :key="db"
                  :label="db"
                  :value="db"
                />
              </el-select>
              <el-button
                :disabled="model.target.shards.length == 0"
                @click="listDatabases()"
              >Connect</el-button>
            </el-form-item>
          </el-form>
        </el-tab-pane>
      </el-tabs>
      <span slot="footer" class="dialog-footer">
        <el-button
          type="primary"
          :disabled="
            model.source.selectedTable.length === 0 ||
              model.source.dsn == '' ||
              model.target.selectedDatabase == ''
          "
          @click="choose"
        >Add</el-button>
        <el-button @click="dialogVisible = false">Cancel</el-button>
      </span>
    </el-dialog>
    <el-dialog
      title="Notice"
      style="font-size: 20px"
      :visible.sync="noticeVisible"
      width="30%"
    >
      <div style="font-size: 15px">
        Do you want add incremental sync and connector at the same time ?
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="doAllAdd">Yes</el-button>
        <el-button @click="add">No</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
export default {
  props: {
    value: {
      type: String,
      default: ''
    }
  },
  data() {
    var opt = {
      defaultTab: 'dmAddSource',
      options: [],
      database: 'all',
      list: [],
      listLoading: false,
      tableKey: 0,
      pager: {
        page: 1,
        total: 0
      },
      dialogVisible: false,
      noticeVisible: false,
      model: {
        source: {
          dsn: '',
          tables: [],
          suffix: '',
          selectedTable: [],
          createTableSql: [],
          queryKey: []
        },
        target: {
          shards: [],
          cluster: '',
          databases: [],
          selectedDatabase: '',
          queryNode: ''
        },
        windowStartHour: '',
        windowEndHour: ''
      }
    }
    this.$store
      .dispatch('datasync/databaseList', {
        service: 'dm'
      })
      .then((response) => {
        opt.options = response
        if (opt.options == null) {
          opt.options = ['all']
        } else {
          opt.options.unshift('all')
          if (localStorage.getItem('dmdb')) {
            opt.database = localStorage.getItem('dmdb')
          }
        }
      })
      .catch((error) => {
        console.log(error)
      })
    return opt
  },
  mounted: function() {
    if (localStorage.getItem('fromCreateTablePage') === 'Yes') {
      this.dialogVisible = true
      localStorage.setItem('fromCreateTablePage', 'No')
    } else {
      this.refresh(1)
    }
  },
  methods: {
    setDbAndReflesh() {
      localStorage.setItem('dmdb', this.database)
      this.refresh(1)
    },
    refresh(page) {
      this.listLoading = true
      if (this.database === '') {
        this.$message({
          message: 'Choose a database before refresh!',
          type: 'error',
          duration: 1500
        })
        this.listLoading = false
        return
      }
      this.$store
        .dispatch('datasync/dmList', {
          page: page,
          size: 10,
          dbName: this.database
        })
        .then((response) => {
          this.list = response.dmList
          this.pager.total = response.pageAndSize.size
        })
        .catch((error) => {
          console.log(error)
        })
      this.listLoading = false
    },
    redo(id) {
      this.$store
        .dispatch('datasync/dmRedo', {
          id: id.toString()
        })
        .then((response) => {
          this.$message({
            message: 'Job has been redo successfully!',
            type: 'success',
            duration: 1500
          })
          this.refresh(this.pager.page)
        })
        .catch((error) => {
          console.log(error)
        })
    },
    stop(id) {
      this.$store
        .dispatch('datasync/dmStop', {
          id: id.toString()
        })
        .then((response) => {
          this.$message({
            message: 'Job has been stopped successfully!',
            type: 'success',
            duration: 1500
          })
          this.refresh(this.pager.page)
        })
        .catch((error) => {
          console.log(error)
        })
    },
    del(id) {
      this.$store
        .dispatch('datasync/dmDelete', {
          id: id.toString()
        })
        .then((response) => {
          this.$message({
            message: 'Job has been deleted successfully!',
            type: 'success',
            duration: 1500
          })
          this.refresh(this.pager.page)
        })
        .catch((error) => {
          console.log(error)
        })
    },
    generateSql() {
      this.$store
        .dispatch('datasync/generateSql', {
          dsn: this.model.source.dsn,
          table: this.model.source.selectedTable,
          database: this.model.target.selectedDatabase
        })
        .then((response) => {
          this.model.source.queryKey = response.queryKey || []
          this.model.source.createTableSql = response.sql || []
          if (response.failedTables && response.failedTables.length) {
            this.model.source.selectedTable = this.model.source.selectedTable.filter(
              (sel) =>
                response.failedTables.findIndex((failed) => failed === sel) ===
                -1
            )
          }
          if (response.failedReasons && response.failedReasons.length) {
            this.$message({
              message: response.failedReasons[0],
              type: 'error',
              duration: 1500
            })
            if (response.info) {
              this.$message({
                message: response.info,
                type: 'error',
                duration: 1500
              })
            }
          }
        })
        .catch((error) => {
          console.log(error)
        })
    },
    listTables() {
      this.$store
        .dispatch('datasync/listTables', {
          string: this.model.source.dsn
        })
        .then((response) => {
          this.model.source.tables = response.stringList
          if (
            response.stringList.length > 0 &&
            this.model.source.selectedTable.length < 1
          ) {
            this.model.source.selectedTable.push(response.stringList[0])
          }
        })
        .catch((error) => {
          console.log(error)
        })
    },
    choose() {
      this.noticeVisible = true
    },
    doAllAdd() {
      if (this.model.windowStartHour > 23 || this.model.windowEndHour > 23) {
        this.$message({
          message: 'Your hour should between 0 and 23!',
          type: 'error',
          duration: 1500
        })
        return
      }
      this.$store
        .dispatch('datasync/dmAdd', {
          dmModel: this.model,
          windowStartHour: this.model.windowStartHour,
          windowEndHour: this.model.windowEndHour
        })
        .then((response) => {
          this.$store
            .dispatch('datasync/connectorAdd', {
              model: this.model
            })
            .then((response) => {
              this.$store
                .dispatch('datasync/rtuAdd', {
                  model: this.model
                })
                .then((response) => {
                  if (this.database === '') {
                    this.database = 'all'
                  }
                  this.refresh(this.pager.page)
                  this.dialogVisible = false
                  this.noticeVisible = false
                  localStorage.setItem('dmModel', JSON.stringify(this.model))
                  this.$message({
                    message:
                      'Full/Incremental Sync Task and connector added successfully!',
                    type: 'success',
                    duration: 1500
                  })
                })
                .catch((error) => {
                  console.log(error)
                })
            })
            .catch((error) => {
              console.log(error)
            })
        })
        .catch((error) => {
          console.log(error)
        })
    },
    execSql() {
      this.$store
        .dispatch('datasync/execSql', {
          sql: this.model.source.createTableSql
        })
        .then((response) => {
          this.$message({
            message: 'Your sql has been executed by clickhouse.',
            type: 'success',
            duration: 1500
          })
        })
        .catch((error) => {
          console.log(error)
        })
    },
    add() {
      if (this.model.windowStartHour > 23 || this.model.windowEndHour > 23) {
        this.$message({
          message: 'Your hour should between 0 and 23!',
          type: 'error',
          duration: 1500
        })
        return
      }
      this.$store
        .dispatch('datasync/dmAdd', {
          dmModel: this.model,
          windowStartHour: this.model.windowStartHour,
          windowEndHour: this.model.windowEndHour
        })
        .then((response) => {
          if (this.database === '') {
            this.database = 'all'
          }
          this.refresh(this.pager.page)
          this.dialogVisible = false
          this.noticeVisible = false
          localStorage.setItem('dmModel', JSON.stringify(this.model))
          this.$message({
            message: 'Full Sync Task added successfully!',
            type: 'success',
            duration: 1500
          })
        })
        .catch((error) => {
          console.log(error)
        })
    },
    listDatabases() {
      this.$store
        .dispatch('datasync/listDatabases', {})
        .then((response) => {
          this.model.target.databases = response.stringList
          if (response.stringList.length > 0) {
            this.model.target.selectedDatabase = response.stringList[0]
          }
        })
        .catch((error) => {
          console.log(error)
        })
    },
    loadModel() {
      this.dialogVisible = true
      var m = localStorage.getItem('dmModel')
      if (m) {
        this.model = JSON.parse(m)
      }
      this.$store
        .dispatch('datasync/defaultConfig', {})
        .then((response) => {
          this.model.target.shards = response.shards
          this.model.target.cluster = response.cluster
          this.model.target.queryNode = response.queryNode
        })
        .catch((error) => {
          console.log(error)
        })
    }
  }
}
</script>

<style scoped>
.pagination-container {
  background: #fff;
  padding: 32px 16px;
  text-align: center;
  margin: 0 auto;
}
.pagination-container.hidden {
  display: none;
}
.button-container {
  display: inline-block;
}
.board {
  width: 1000px;
  margin-left: 20px;
  display: flex;
  justify-content: space-around;
  flex-direction: row;
  align-items: flex-start;
}
</style>
