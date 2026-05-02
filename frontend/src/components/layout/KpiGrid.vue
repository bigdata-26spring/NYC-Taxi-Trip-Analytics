<template>
  <section class="kpi-grid">
    <article v-for="item in items" :key="item.label" class="kpi-card">
      <div class="kpi-card__label">{{ item.label }}</div>
      <div class="kpi-card__value">{{ item.value }}</div>
      <div class="kpi-card__note">{{ item.note }}</div>
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
