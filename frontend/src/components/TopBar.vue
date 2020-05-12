
<template>
    <div>
        <b-navbar type="dark" variant="dark">
            <b-navbar-brand tag="h1" class="mb-0">Data Vault Architect</b-navbar-brand>
            <b-navbar-nav class="ml-auto">
                <b-nav-item v-for="routes in links"
                    v-bind:key="routes.id"
                    :to="`${routes.page}`" right>{{routes.text}}</b-nav-item>
            </b-navbar-nav>
        </b-navbar>
    </div>
</template>

<script>
export default {
  name: 'Navigation',
  data() {
    return {
      links: [
        {
          id: 0,
          text: 'Databases',
          page: '/database',
        },
        {
          id: 1,
          text: 'Mappings',
          page: '/mapping',
        },
        {
          id: 2,
          text: 'Engine',
          page: '/engine',
        },
        {
          id: 3,
          text: 'About',
          page: '/about',
        },
      ],
    };
  },
};
</script>
