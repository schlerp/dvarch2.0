<template>
  <b-row style='height: calc(100vh - 50px);'>
    <b-col>
      <!-- <b-row class='justify-content-md-center'>
        <b-col>
          <h2>Database Definitions</h2>
        </b-col>
      </b-row> -->
      <b-row class="mb-3">
        <b-col class="col-4">
          <h4>Engine Config</h4>
          <b-form @submit="onSubmitConfig" v-if="showConfig">
            <b-form-group
              id="input-group-config-delim"
              label="Business Key Delimiter:"
              label-for="input-config-delim"
            >
              <b-form-select
                id="input-config-delim"
                v-model="engine_config.bk_delim"
                :options="bk_delims"
                required
              ></b-form-select>
            </b-form-group>

            <b-form-group
              id="input-group-config-bkdtype"
              label="Business Key Data Type:"
              label-for="input-config-bkdtype"
            >
              <b-form-input
                id="input-config-bkdtype"
                v-model="engine_config.bk_dtype"
                required
                placeholder="Business Key Datatype"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-config-hashfunc"
              label="Hashing Function:"
              label-for="input-config-hashfunc"
            >
              <b-form-select
                id="input-config-hashfunc"
                v-model="engine_config.hash_func"
                :options="hash_funcs"
                required
              ></b-form-select>
            </b-form-group>

            <b-form-group
              id="input-group-config-hkdtype"
              label="Hash Columns Data Type:"
              label-for="input-config-hkdtype"
            >
              <b-form-input
                id="input-config-hkdtype"
                v-model="engine_config.hk_dtype"
                required
                placeholder="Hash Columns Datatype"
              ></b-form-input>
            </b-form-group>

            <b-form-group
              id="input-group-config-recsrcdtype"
              label="Record Source Data Type:"
              label-for="input-config-recsrcdtype"
            >
              <b-form-input
                id="input-config-recsrcdtype"
                v-model="engine_config.rec_src_dtype"
                required
                placeholder="Record Source Datatype"
              ></b-form-input>
            </b-form-group>

            <b-button type="submit" variant="primary">Update Config</b-button>
          </b-form>
        </b-col>
        <b-col class="col-8">
          <b-row class="no-gutters">
            <h4>Engine Output</h4>
          </b-row>
          <b-row class="mb-3 no-gutters">
            <b-form-textarea
              id="engineOutputTextArea"
              v-model="engineOutput"
              ref="engineOutputTextArea"
              placeholder="Output will appear here when you run the engine..."
              readonly
              rows="5"
              style="background-color: #ffffff;"
            ></b-form-textarea>
          </b-row>
          <b-row class="no-gutters">
            <b-col>
              <b-button @click="clickRunEngine" variant="info">Run Engine</b-button>
            </b-col>
            <b-col>
              <b-button @click="clickClearHistory" variant="danger">Clear History</b-button>
            </b-col>
          </b-row>
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
      engine_config: {
        bk_delim: '|',
        bk_dtype: 'NVARCHAR(256)',
        hash_func: 'SHA1',
        hk_dtype: 'BINARY(64)',
        rec_src_dtype: 'NVARCHAR(256)',
      },
      showConfig: true,
      bk_delims: ['|', '~', '^', '!', '%'],
      hash_funcs: ['MD5', 'SHA1', 'SHA2_256', 'SHA2_512'],
      engineOutput: '',
    };
  },
  methods: {
    getEngineConfig() {
      this.getEngineConfigFromBackend();
    },
    getEngineConfigFromBackend() {
      const path = 'http://localhost:5000/api/get_engine_config';
      axios.get(path)
        .then((response) => {
          // eslint-disable-next-line no-console
          // console.log(response.data);
          this.engine_config = response.data;
        });
      // .catch((error) => {
      //   // eslint-disable-next-line no-console
      //   console.log(error);
      // });
    },
    onSubmitConfig(evt) {
      evt.preventDefault();
      // eslint-disable-next-line no-alert
      // alert(JSON.stringify(this.engine_config));
      const path = 'http://localhost:5000/api/save_engine_config';
      const config = {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': 'http://localhost:8080',
        },
      };
      axios.post(path, this.engine_config, config);
    },
    clickRunEngine() {
      this.logUpdateLoop();
      const path = 'http://localhost:5000/api/run_engine';
      const config = {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': 'http://localhost:8080',
        },
      };
      axios.post(path, true, config);
    },
    clickClearHistory() {
      this.engineOutput = '';
      const path = 'http://localhost:5000/api/clear_engine_log';
      const config = {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': 'http://localhost:8080',
        },
      };
      axios.post(path, true, config);
    },
    scrollToBottom() {
      document.getElementById('engineOutputTextArea').scrollTop =
        document.getElementById('engineOutputTextArea').scrollHeight;
    },
    fetchEngineLogs() {
      const path = 'http://localhost:5000/api/get_engine_log_latest';
      axios.get(path)
        .then((response) => {
          // eslint-disable-next-line no-console
          console.log(response.data);
          this.engineOutput += response.data;
          document.getElementById('engineOutputTextArea').scrollTop =
            document.getElementById('engineOutputTextArea').scrollHeight;
        })
        .catch((error) => {
          // eslint-disable-next-line no-console
          console.log(error);
        });
    },
    logUpdateLoop() {
      // check if changed every 30 seconds and save if so
      setInterval(() => { this.fetchEngineLogs(); }, 2500);
    },
  },
  created() {
    this.getEngineConfig();
  },
};
</script>
