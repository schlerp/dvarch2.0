<template>
  <b-row style='height: calc(100vh - 50px);'>
    <b-col>
      <b-row class='justify-content-md-center'>
        <b-col>
          <h2>Database Definitions</h2>
        </b-col>
      </b-row>
      <b-row class="mb-3">
        <b-col>
          <b-form @submit="onSubmitSource" v-if="showSource">
            <b-form-group
              id="input-group-source-database"
              label="Source Database:"
              label-for="input-source-database"
            >
              <b-form-input
                id="input-source-database"
                v-model="formSource.database"
                required
                placeholder="Source Database"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-source-server"
              label="Source Database Server:"
              label-for="input-source-server"
              description="Path to you source data base (including instance for MSSQL)"
            >
              <b-form-input
                id="input-source-server"
                v-model="formSource.server"
                required
                placeholder="Source Database Server"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-source-port"
              label="Source Database Port:"
              label-for="input-source-port"
            >
              <b-form-input
                id="input-source-port"
                v-model="formSource.port"
                type="number"
                required
                placeholder="Source Database port"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-source-user"
              label="Source Database User:"
              label-for="input-source-user"
            >
              <b-form-input
                id="input-source-user"
                v-model="formSource.user"
                required
                placeholder="Source Database User"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-source-password"
              label="Source Database Password:"
              label-for="input-source-password"
            >
              <b-form-input
                id="input-source-password"
                v-model="formSource.password"
                type="password"
                required
                placeholder="Source Database Password"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-source-type"
              label="Source Database Type:"
              label-for="input-source-type"
            >
              <b-form-select
                id="input-source-type"
                v-model="formSource.type"
                :options="types"
                required
              ></b-form-select>
            </b-form-group>

            <b-button type="submit" variant="primary">Submit</b-button>
          </b-form>
        </b-col>

        <b-col>
          <b-form @submit="onSubmitTarget" v-if="showTarget">
            <b-form-group
              id="input-group-target-database"
              label="Target Database:"
              label-for="input-target-database"
            >
              <b-form-input
                id="input-target-database"
                v-model="formTarget.database"
                required
                placeholder="Target Database"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-target-server"
              label="Target Database Server:"
              label-for="input-target-server"
              description="Path to you target data base (including instance for MSSQL)"
            >
              <b-form-input
                id="input-target-server"
                v-model="formTarget.server"
                required
                placeholder="Target Database Server"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-target-port"
              label="Target Database Port:"
              label-for="input-target-port"
            >
              <b-form-input
                id="input-target-port"
                v-model="formTarget.port"
                type="number"
                required
                placeholder="Target Database port"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-target-user"
              label="Target Database User:"
              label-for="input-target-user"
            >
              <b-form-input
                id="input-target-user"
                v-model="formTarget.user"
                required
                placeholder="Target Database User"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-target-password"
              label="Target Database Password:"
              label-for="input-target-password"
            >
              <b-form-input
                id="input-target-password"
                v-model="formTarget.password"
                type="password"
                required
                placeholder="Target Database Password"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-target-type"
              label="Target Database Type:"
              label-for="input-target-type"
            >
              <b-form-select
                id="input-target-type"
                v-model="formTarget.type"
                :options="types"
                required
              ></b-form-select>
            </b-form-group>

            <b-button type="submit" variant="primary">Submit</b-button>
          </b-form>
        </b-col>
      </b-row>
    </b-col>
  </b-row>
</template>

<script>
import axios from 'axios';

export default {
  data() {
    return {
      formSource: {
        database: '',
        server: '',
        type: 'MSSQL',
        port: 1433,
        user: 'sa',
        password: '',
      },
      showSource: true,
      formTarget: {
        database: '',
        server: '',
        type: 'MSSQL',
        port: 1433,
        user: 'sa',
        password: '',
      },
      showTarget: true,
      types: ['MSSQL'],
    };
  },
  methods: {
    getSourceDatabase() {
      this.json = this.getSourceDatabaseFromBackend();
    },
    getSourceDatabaseFromBackend() {
      const path = 'http://localhost:5000/api/get_source_database';
      axios.get(path)
        .then((response) => {
          this.formSource = response.data;
        })
        .catch((error) => {
          // eslint-disable-next-line no-console
          console.log(error);
        });
    },
    getTargetDatabase() {
      this.json = this.getTargetDatabaseFromBackend();
    },
    getTargetDatabaseFromBackend() {
      const path = 'http://localhost:5000/api/get_target_database';
      axios.get(path)
        .then((response) => {
          this.formTarget = response.data;
        })
        .catch((error) => {
          // eslint-disable-next-line no-console
          console.log(error);
        });
    },
    onSubmitSource(evt) {
      evt.preventDefault();
      // eslint-disable-next-line no-alert
      alert(JSON.stringify(this.formSource));
      const path = 'http://localhost:5000/api/save_source_database';
      const config = {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': 'http://localhost:8080',
        },
      };
      axios.post(path, this.formSource, config);
    },
    onSubmitTarget(evt) {
      evt.preventDefault();
      // eslint-disable-next-line no-alert
      alert(JSON.stringify(this.formTarget));
      const path = 'http://localhost:5000/api/save_target_database';
      const config = {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': 'http://localhost:8080',
        },
      };
      axios.post(path, this.formTarget, config);
    },
  },
  created() {
    this.getSourceDatabase();
    this.getTargetDatabase();
  },
};
</script>
