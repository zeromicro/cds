<template>
  <div class="app-container">
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
          <el-input
            v-for="(item, index) in model.source.createTableSql"
            :key="index"
            type="textarea"
            :value="item"
            :rows="10"
          />
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
          model.source.dsn == '' ||
            model.source.selectedTable.length == 0 ||
            model.target.selectedDatabase == ''
        "
        @click="generateSql()"
      >
        Generate Create Table SQL
      </el-button>
      <el-button
        type="primary"
        :disabled="!model.source.createTableSql.length > 0"
        @click="showDiag()"
      >
        Send SQL To Clickhouse
      </el-button>
    </span>
    <el-dialog
      title="Notice"
      style="font-size: 20px"
      :visible.sync="dialogVisible"
      width="30%"
    >
      <div style="font-size: 15px">
        Do you want continue and add Full Sync at the same time ?
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" @click="jumpToDm">Yes</el-button>
        <el-button @click="execSql">No</el-button>
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
      dialogVisible: false,
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
    this.loadModel()
  },
  methods: {
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
    showDiag() {
      this.dialogVisible = true
    },
    jumpToDm() {
      this.$store
        .dispatch('datasync/execSql', {
          sql: this.model.source.createTableSql
        })
        .then((response) => {
          localStorage.setItem('dmModel', JSON.stringify(this.model))
          this.dialogVisible = false
          localStorage.setItem('fromCreateTablePage', 'Yes')
          window.location.href = '/#/datasync/dm'
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
          localStorage.setItem('dmModel', JSON.stringify(this.model))
          this.dialogVisible = false
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
