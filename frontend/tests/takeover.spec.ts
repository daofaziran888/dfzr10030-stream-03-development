import { expect, test } from '@playwright/test'

// 后端状态在整套 E2E 中是共享的，这里只验证页面能正常渲染并与接管接口联通，
// 具体的交接判定由 backend/tests/test_source_takeover.py 覆盖。
test('源端接管校验：校验表单、批次清单与续读位置渲染', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', e => errors.push(e.message))
  page.on('response', r => { if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`) })

  await page.goto('/source_takeover')
  await expect(page.getByTestId('prepare-submit')).toBeVisible()
  await expect(page.getByTestId('prepare-源端编号')).toHaveValue('SOUR-MYSQL-01')
  await expect(page.getByTestId('prepare-表名')).toHaveValue('orders')
  // 表头与各来源续读位置区块都已渲染。
  await expect(page.locator('.data-table').first()).toBeVisible()
  await expect(page.locator('.section-title')).toHaveText('各来源续读位置')
  await expect(page.locator('.data-table').nth(1)).toBeVisible()
  expect(errors).toEqual([])
})
