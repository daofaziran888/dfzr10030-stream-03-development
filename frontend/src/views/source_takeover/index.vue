<template>
  <section class="page" data-module="source_takeover">
    <header class="page-head">
      <div>
        <h2>源端接管校验</h2>
        <p class="page-desc">换绑时依据源端编号、表名和采集区间核对两端事件，生成供 CDC 续读的接管位点批次清单；确认以来源 + 表名为粒度推进。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="run-check" type="button" :disabled="busy" @click="runCheck">
          重新核验交接
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="analysis-form" @submit.prevent="prepare">
      <label>
        <span>源端编号</span>
        <input v-model="form.源端编号" data-testid="prepare-源端编号" placeholder="SOUR-MYSQL-01" />
      </label>
      <label>
        <span>表名</span>
        <input v-model="form.表名" data-testid="prepare-表名" placeholder="orders" />
      </label>
      <label>
        <span>采集开始位点</span>
        <input v-model="form.开始位点" data-testid="prepare-开始位点" placeholder="0" />
      </label>
      <label>
        <span>采集结束位点</span>
        <input v-model="form.结束位点" data-testid="prepare-结束位点" placeholder="10000" />
      </label>
      <button class="btn primary" data-testid="prepare-submit" type="submit" :disabled="busy">
        接管校验并生成续读批次
      </button>
    </form>

    <div v-if="message" class="notice" :class="{ error: messageIsError }" data-testid="takeover-message">{{ message }}</div>

    <table class="data-table">
      <thead>
        <tr>
          <th>批次编号</th>
          <th>源端编号</th>
          <th>表名</th>
          <th>旧绑定 / 区间</th>
          <th>新绑定 / 区间</th>
          <th>已核对区间</th>
          <th>整表接管时点</th>
          <th>未确认区间</th>
          <th>状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="batch in batches"
          :key="String(batch.id)"
          :data-testid="`batch-${batch.id}`"
          :data-status="String(batch.状态)"
        >
          <td>{{ batch.批次编号 }}</td>
          <td>{{ batch.源端编号 }}</td>
          <td>{{ batch.表名 }}</td>
          <td>{{ batch.旧绑定.绑定编号 }}<br /><small>{{ spanText(batch.旧端区间) }}</small></td>
          <td>{{ batch.新绑定.绑定编号 }}<br /><small>{{ spanText(batch.新端区间) }}</small></td>
          <td>{{ spanText(batch.已核对区间) }}</td>
          <td>{{ batch.整表接管时点 }}</td>
          <td :class="{ 'error-text': batch.未确认区间.length }">{{ unconfirmedText(batch) }}</td>
          <td>{{ batch.状态 }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`resume-${batch.id}`" @click="resume(batch)">续读</button>
            <button class="link" type="button" :data-testid="`confirm-${batch.id}`" @click="confirm(batch)">确认接管</button>
            <button class="link" type="button" :data-testid="`backfill-${batch.id}`" @click="backfill(batch)">旧端补齐</button>
          </td>
        </tr>
        <tr v-if="!batches.length">
          <td :colspan="10" class="empty-state">暂无接管批次，可先按来源和表名发起接管校验</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">各来源续读位置</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>源端编号</th>
          <th>表名</th>
          <th>确认位点</th>
          <th>数据游标</th>
          <th>已消费变更</th>
          <th>仍未由旧绑定闭合的区间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="cursor in cursors" :key="`${cursor.源端编号}-${cursor.表名}`" :data-testid="`cursor-${cursor.源端编号}-${cursor.表名}`">
          <td>{{ cursor.源端编号 }}</td>
          <td>{{ cursor.表名 }}</td>
          <td>{{ cursor.确认位点 }}</td>
          <td>{{ cursor.数据游标 }}</td>
          <td>{{ (cursor.已消费变更 || []).join('、') || '—' }}</td>
          <td>{{ cursor.仍由旧绑定负责.length ? cursor.仍由旧绑定负责.map(unconfirmedText).join('；') : '—' }}</td>
        </tr>
      </tbody>
    </table>

    <div v-if="issues.length" class="notice error" data-testid="consistency-issues">
      交接清单回查发现问题：{{ issues.join('；') }}
    </div>

    <footer class="page-foot">
      <span>共 {{ batches.length }} 个接管批次</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Batch = {
  id: number
  [key: string]: any
}

type Cursor = {
  源端编号: string
  表名: string
  确认位点: number | null
  数据游标: number | null
  已消费变更: string[]
  仍由旧绑定负责: Record<string, any>[]
}

const ENDPOINT = '/api/source_takeover'
const batches = ref<Batch[]>([])
const cursors = ref<Cursor[]>([])
const issues = ref<string[]>([])
const message = ref('')
const messageIsError = ref(false)
const errorMessage = ref('')
const busy = ref(false)
const form = ref({ 源端编号: 'SOUR-MYSQL-01', 表名: 'orders', 开始位点: '0', 结束位点: '10000' })

const stats = computed(() => [
  { label: '接管批次', value: batches.value.length },
  { label: '待旧端补齐', value: batches.value.filter((b) => b.状态 === '待旧端补齐').length },
  { label: '仍由旧绑定负责', value: cursors.value.reduce((sum, c) => sum + c.仍由旧绑定负责.length, 0) },
])

function spanText(span: any) {
  if (!span) return '—'
  return `${span.起始位点 ?? '—'} ~ ${span.结束位点 ?? '—'}`
}

function unconfirmedText(item: any) {
  return `${item.绑定编号 || ''} ${item.起始位点} ~ ${item.结束位点 ?? '尾位点未交出'}`
}

async function post(path: string, body?: unknown) {
  const response = await request(`${ENDPOINT}${path}`, {
    method: 'POST',
    body: JSON.stringify(body ?? {}),
  })
  const payload = await response.json()
  if (!response.ok) throw new Error(payload.detail || '接口返回异常')
  return payload
}

function flash(ok: boolean, text: string) {
  message.value = text
  messageIsError.value = !ok
}

async function reload() {
  errorMessage.value = ''
  try {
    const [batchRes, cursorRes, checkRes] = await Promise.all([
      request(`${ENDPOINT}/batches`),
      request(`${ENDPOINT}/cursors`),
      request(`${ENDPOINT}/check`),
    ])
    batches.value = ((await batchRes.json()).items ?? []) as Batch[]
    cursors.value = ((await cursorRes.json()).items ?? []) as Cursor[]
    const check = await checkRes.json()
    issues.value = check?.一致性?.问题 ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接管清单读取失败'
  }
}

async function prepare() {
  busy.value = true
  try {
    const payload = await post('/prepare', {
      源端编号: form.value.源端编号,
      表名: form.value.表名,
      采集区间: { 开始位点: form.value.开始位点, 结束位点: form.value.结束位点 },
    })
    flash(payload.ok, payload.message)
    await reload()
  } catch (error) {
    flash(false, error instanceof Error ? error.message : '接管校验失败')
  } finally {
    busy.value = false
  }
}

async function resume(batch: Batch) {
  try {
    const payload = await post(`/batches/${batch.id}/resume`)
    flash(payload.ok, payload.message)
    await reload()
  } catch (error) {
    flash(false, error instanceof Error ? error.message : '续读失败')
  }
}

async function confirm(batch: Batch) {
  try {
    const payload = await post(`/batches/${batch.id}/confirm`)
    flash(payload.ok, payload.message)
    await reload()
  } catch (error) {
    flash(false, error instanceof Error ? error.message : '接管确认失败')
  }
}

async function backfill(batch: Batch) {
  try {
    const tail = batch.新端区间?.结束位点 ?? batch.整表接管时点
    const payload = await post(`/batches/${batch.id}/backfill`, { 尾位点: tail })
    flash(payload.ok, payload.message)
    await reload()
  } catch (error) {
    flash(false, error instanceof Error ? error.message : '旧端补齐失败')
  }
}

async function runCheck() {
  try {
    const payload = await post('/restart')
    issues.value = payload?.一致性?.问题 ?? []
    flash(payload?.一致性?.正常 !== false, '交接清单回查完成')
    await reload()
  } catch (error) {
    flash(false, error instanceof Error ? error.message : '交接回查失败')
  }
}

onMounted(reload)
</script>
