<template>
  <section class="metric-strip">
    <article v-for="item in items" :key="item.label" class="metric-tile">
      <div class="metric-tile__label">{{ item.label }}</div>
      <div class="metric-tile__value">{{ item.value }}</div>
      <div class="metric-tile__note">{{ item.note }}</div>
    </article>
  </section>
</template>

<script setup>
defineProps({
  items: {
    type: Array,
    default: () => [],
  },
});
</script>
