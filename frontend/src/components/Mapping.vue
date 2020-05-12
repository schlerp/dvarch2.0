<template>

  <b-row style='height: calc(100vh - 50px);'>

    <b-alert
      class="position-fixed fixed-bottom m-0 rounded-0"
      style="z-index: 2000;"
      :show="dismissCountDown"
      dismissible
      fade
      variant="success"
      @dismissed="dismissCountDown=0"
      @dismiss-count-down="countDownChanged"
    >
      Mappings Saved!
    </b-alert>

      <v-jsoneditor
        ref="vjeditor"
        v-model="json"
        :options="options"
        :plus="true"/>
  </b-row>
</template>

<script>
import axios from 'axios';

export default {
  data() {
    return {
      options: {
        mode: 'code',
        onChange: this.onChange,
      },
      json: {
        temp: 'placeholder',
      },
      changed: false,
      dismissSecs: 5,
      dismissCountDown: 0,
    };
  },
  methods: {
    onChange() {
      // on changes set change to true
      try {
        // this will fail if json is invalid
        this.$refs.vjeditor.editor.get();
        this.changed = true;
      } catch (error) {
        this.changed = false;
      }
    },
    autoSaveMapping() {
      // // eslint-disable-next-line no-console
      // console.log('checking for save');
      if (this.changed === true) {
        // // eslint-disable-next-line no-console
        // console.log('saving due to changes');
        this.postMappingToBackend();
        this.changed = false;
        this.showAlert();
      }
    },
    saveLoop() {
      // check if changed every 30 seconds and save if so
      setInterval(() => { this.autoSaveMapping(); }, 30000);
    },
    getMapping() {
      this.json = this.getMappingFromBackend();
    },
    getMappingFromBackend() {
      const path = 'http://localhost:5000/api/get_mapping';
      axios.get(path)
        .then((response) => {
          this.json = response.data;
        });
      // .catch((error) => {
      //   // eslint-disable-next-line no-console
      //   console.log(error);
      // });
    },
    postMappingToBackend() {
      const path = 'http://localhost:5000/api/save_mapping';
      const config = {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': 'http://localhost:8080',
        },
      };
      axios.post(path, this.json, config);
      // .then((response) => {
      //   // eslint-disable-next-line no-console
      //   console.log(response);
      // })
      // .catch((error) => {
      //   // eslint-disable-next-line no-console
      //   console.log(error);
      // });
    },
    countDownChanged(dismissCountDown) {
      this.dismissCountDown = dismissCountDown;
    },
    showAlert() {
      this.dismissCountDown = this.dismissSecs;
    },
  },
  created() {
    this.getMapping();
    this.saveLoop();
  },
};
</script>
