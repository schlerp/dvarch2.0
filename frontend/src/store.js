import Vue from 'vue';
import Vuex from 'vuex';

Vue.use(Vuex);

export default new Vuex.Store({
  state: {
    log_text: '',
  },
  mutations: {
    appendLog(state, msg) {
      state.log_text += msg;
    },
    clearLog(state) {
      state.log_text = '';
    },
  },
});
