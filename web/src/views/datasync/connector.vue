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
      <el-table-column label="ID" sortable prop="id" align="center">
        <template slot-scope="{ row }">
          <span>{{ row.sourceId }}</span>
        </template>
      </el-table-column>
      <el-table-column label="Database" sortable prop="name" align="center">
        <template slot-scope="{ row }">
          <span>{{ row.sourceDb }}</span>
        </template>
      </el-table-column>
      <el-table-column label="Table" sortable prop="status" align="center">
        <template slot-scope="{ row }">
          <span>{{ row.sourceTable }}</span>
        </template>
      </el-table-column>
      <el-table-column label="Type" prop="information" align="center">
        <template slot-scope="{ row }">
          <span>{{ row.sourceType }}</span>
        </template>
      </el-table-column>
      <el-table-column
        label="Create Time"
        sortable
        prop="createTime"
        align="center"
      >
        <template slot-scope="{ row }">
          <span>{{ row.createTime }}</span>
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
          <el-tooltip effect="light" placement="bottom" content="delete">
            <el-button
              type="text"
              class="el-icon-delete"
              @click="del(row.sourceId, row.sourceType)"
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
      title="Add Connector"
      :close-on-click-modal="false"
      :visible.sync="dialogVisible"
      top="1cm"
    >
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
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button
          type="primary"
          :disabled="
            model.source.selectedTable.length == 0 || model.source.dsn == ''
          "
          @click="add"
        >Add</el-button>
        <el-button @click="dialogVisible = false">Cancel</el-button>
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
      model: {
        source: {
          dsn: '',
          tables: [],
          suffix: '',
          selectedTable: [],
          createTableSql: [],
          queryKey: []
        }
      }
    }
    this.$store
      .dispatch('datasync/databaseList', {
        service: 'connector'
      })
      .then((response) => {
        opt.options = response
        if (opt.options == null) {
          opt.options = ['all']
        } else {
          opt.options.unshift('all')
          if (localStorage.getItem('connectordb')) {
            opt.database = localStorage.getItem('connectordb')
          }
        }
      })
      .catch((error) => {
        console.log(error)
      })
    return opt
  },
  mounted: function() {
    this.refresh(1)
  },
  methods: {
    setDbAndReflesh() {
      localStorage.setItem('connectordb', this.database)
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
        .dispatch('datasync/connectorList', {
          page: page,
          size: 10,
          dbName: this.database
        })
        .then((response) => {
          this.list = response.connectorList
          this.pager.total = response.pageAndSize.size
        })
        .catch((error) => {
          console.log(error)
        })
      this.listLoading = false
    },
    del(sourceId, type) {
      this.$store
        .dispatch('datasync/connectorDelete', {
          sourceId: sourceId,
          type: type
        })
        .then((response) => {
          this.$message({
            message: 'Connector has been deleted successfully!',
            type: 'success',
            duration: 1500
          })
          this.refresh(this.pager.page)
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
    add() {
      this.$store
        .dispatch('datasync/connectorAdd', {
          model: this.model
        })
        .then((response) => {
          if (this.database === '') {
            this.database = 'all'
          }
          this.refresh(this.pager.page)
          this.dialogVisible = false
          localStorage.setItem('connectorModel', JSON.stringify(this.model))
        })
        .catch((error) => {
          console.log(error)
        })
    },
    loadModel() {
      this.dialogVisible = true
      var m = localStorage.getItem('connectorModel')
      if (m) {
        this.model = JSON.parse(m)
      }
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
