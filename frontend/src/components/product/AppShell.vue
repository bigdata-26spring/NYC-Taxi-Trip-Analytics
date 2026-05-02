<template>
  <div class="product-shell">
    <aside class="product-sidebar">
      <div class="product-brand">
        <div class="product-brand__mark">NYC</div>
        <div>
          <p class="eyebrow">Taxi Operations</p>
          <h1>Intelligence Console</h1>
        </div>
      </div>

      <nav class="product-nav" aria-label="Application sections">
        <button
          v-for="item in navItems"
          :key="item.key"
          type="button"
          :class="['product-nav__item', { active: activeView === item.key }]"
          @click="$emit('update:activeView', item.key)"
        >
          <component :is="item.icon" :size="18" />
          <span>{{ item.label }}</span>
        </button>
      </nav>

      <div class="product-sidebar__footer">
        <span>Dataset</span>
        <strong>Yellow Taxi 2021-2024</strong>
      </div>
    </aside>

    <main class="product-main">
      <slot />
    </main>
  </div>
</template>

<script setup>
defineProps({
  navItems: {
    type: Array,
    required: true,
  },
  activeView: {
    type: String,
    required: true,
  },
});

defineEmits(["update:activeView"]);
</script>
