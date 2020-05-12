<template>
  <div id="app">
    <TopBar/>
    <b-container fluid="lg">
      <router-view/>
    </b-container>
  </div>
</template>

<script>
import TopBar from './components/TopBar';

export default {
  name: 'App',
  components: {
    // eslint-disable-next-line quote-props
    'TopBar': TopBar,
  },
};
</script>

<style>
#app {
  font-family: 'Inconsolata', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
}
</style>
