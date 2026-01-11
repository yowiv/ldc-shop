'use server'

import { db } from "@/lib/db"
import { cards, orders, refundRequests, loginUsers } from "@/lib/db/schema"
import { and, eq, sql } from "drizzle-orm"
import { revalidatePath } from "next/cache"

export async function getRefundParams(orderId: string) {
    // Auth Check
    const { auth } = await import("@/lib/auth")
    const session = await auth()
    const user = session?.user
    const adminUsers = process.env.ADMIN_USERS?.toLowerCase().split(',') || []
    if (!user || !user.username || !adminUsers.includes(user.username.toLowerCase())) {
        throw new Error("Unauthorized")
    }

    // Get Order
    const order = await db.query.orders.findFirst({
        where: eq(orders.orderId, orderId)
    })

    if (!order) throw new Error("Order not found")
    if (!order.tradeNo) throw new Error("Missing trade_no")

    // Return params for client-side form submission
    return {
        pid: process.env.MERCHANT_ID!,
        key: process.env.MERCHANT_KEY!,
        trade_no: order.tradeNo,
        out_trade_no: order.orderId,
        money: Number(order.amount).toFixed(2)
    }
}

export async function markOrderRefunded(orderId: string) {
    // Auth Check
    const { auth } = await import("@/lib/auth")
    const session = await auth()
    const user = session?.user
    const adminUsers = process.env.ADMIN_USERS?.toLowerCase().split(',') || []
    if (!user || !user.username || !adminUsers.includes(user.username.toLowerCase())) {
        throw new Error("Unauthorized")
    }

    await db.transaction(async (tx: any) => {
        const order = await tx.query.orders.findFirst({ where: eq(orders.orderId, orderId) })
        if (!order) throw new Error("Order not found")

        // Refund points if used
        if (order.userId && order.pointsUsed && order.pointsUsed > 0) {
            await tx.update(loginUsers)
                .set({ points: sql`${loginUsers.points} + ${order.pointsUsed}` })
                .where(eq(loginUsers.userId, order.userId))
        }

        // Update order status
        await tx.update(orders).set({ status: 'refunded' }).where(eq(orders.orderId, orderId))

        // Reclaim card back to stock (best effort)
        if (order.cardKey) {
            try {
                await tx.update(cards).set({ isUsed: false, usedAt: null })
                    .where(and(eq(cards.productId, order.productId), eq(cards.cardKey, order.cardKey)))
            } catch {
                // ignore
            }
        }

        // Mark refund request processed if table exists
        try {
            await tx.update(refundRequests).set({ status: 'processed', processedAt: new Date(), updatedAt: new Date() })
                .where(eq(refundRequests.orderId, orderId))
        } catch {
            // ignore (table may not exist)
        }
    })

    revalidatePath('/admin/orders')
    revalidatePath('/admin/refunds')
    revalidatePath(`/order/${orderId}`)

    return { success: true }
}

export async function proxyRefund(orderId: string) {
    // Auth Check
    const { auth } = await import("@/lib/auth")
    const session = await auth()
    const user = session?.user
    const adminUsers = process.env.ADMIN_USERS?.toLowerCase().split(',') || []
    if (!user || !user.username || !adminUsers.includes(user.username.toLowerCase())) {
        throw new Error("Unauthorized")
    }

    const pid = process.env.MERCHANT_ID
    const key = process.env.MERCHANT_KEY
    if (!pid || !key) throw new Error("Missing merchant config")

    const order = await db.query.orders.findFirst({ where: eq(orders.orderId, orderId) })
    if (!order) throw new Error("Order not found")
    if (!order.tradeNo) throw new Error("Missing trade_no")

    const body = new URLSearchParams({
        pid,
        key,
        trade_no: order.tradeNo,
        out_trade_no: order.orderId,
        money: Number(order.amount).toFixed(2),
    })

    const resp = await fetch('https://credit.linux.do/epay/api.php', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body
    })

    const text = await resp.text()

    let success = false
    try {
        const json = JSON.parse(text)
        success = json?.code === 1 || json?.status === 'success' || json?.msg === 'success'
    } catch {
        success = /success/i.test(text)
    }

    if (!resp.ok) {
        throw new Error(`Refund proxy failed (${resp.status})`)
    }

    if (success) {
        await markOrderRefunded(orderId)
        return { ok: true, processed: true, message: text.slice(0, 500) }
    }

    return { ok: true, processed: false, message: text.slice(0, 500) }
}
