import { db } from "@/lib/db";
import { orders, cards } from "@/lib/db/schema";
import { eq, sql } from "drizzle-orm";

export async function processOrderFulfillment(orderId: string, paidAmount: number, tradeNo: string) {
    const order = await db.query.orders.findFirst({
        where: eq(orders.orderId, orderId)
    });

    if (!order) {
        throw new Error(`Order ${orderId} not found`);
    }

    // Verify Amount (Prevent penny-dropping)
    const orderMoney = parseFloat(order.amount);

    // Allow small float epsilon difference
    if (Math.abs(paidAmount - orderMoney) > 0.01) {
        throw new Error(`Amount mismatch! Order: ${orderMoney}, Paid: ${paidAmount}`);
    }

    if (order.status === 'pending' || order.status === 'cancelled') {
        await db.transaction(async (tx: any) => {
            // Atomic update to claim card (Postgres only)
            let cardKey: string | undefined;
            let supportsReservation = true;

            try {
                // Try to claim reserved card first
                const reservedResult = await tx.execute(sql`
                    UPDATE cards
                    SET is_used = true,
                        used_at = NOW(),
                        reserved_order_id = NULL,
                        reserved_at = NULL
                    WHERE reserved_order_id = ${orderId} AND COALESCE(is_used, false) = false
                    RETURNING card_key
                `);

                cardKey = reservedResult.rows[0]?.card_key as string | undefined;
            } catch (error: any) {
                const errorString = JSON.stringify(error);
                if (
                    error?.message?.includes('reserved_order_id') ||
                    error?.message?.includes('reserved_at') ||
                    errorString.includes('42703')
                ) {
                    supportsReservation = false;
                } else {
                    throw error;
                }
            }

            if (!cardKey) {
                if (supportsReservation) {
                    // Try to claim strictly available card (not reserved)
                    // Or "stealable" card (reserved long ago) - aligning with notify logic
                    const result = await tx.execute(sql`
                        UPDATE cards
                        SET is_used = true,
                            used_at = NOW(),
                            reserved_order_id = NULL,
                            reserved_at = NULL
                        WHERE id = (
                            SELECT id
                            FROM cards
                            WHERE product_id = ${order.productId}
                              AND COALESCE(is_used, false) = false
                              AND (reserved_at IS NULL OR reserved_at < NOW() - INTERVAL '1 minute')
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING card_key
                    `);

                    cardKey = result.rows[0]?.card_key as string | undefined;
                } else {
                    // Legacy fallback
                    const result = await tx.execute(sql`
                        UPDATE cards
                        SET is_used = true, used_at = NOW()
                        WHERE id = (
                            SELECT id
                            FROM cards
                            WHERE product_id = ${order.productId} AND COALESCE(is_used, false) = false
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING card_key
                    `);

                    cardKey = result.rows[0]?.card_key as string | undefined;
                }
            }

            console.log(`[Fulfill] Order ${orderId}: Card claimed:`, cardKey ? "YES" : "NO");

            if (cardKey) {
                await tx.update(orders)
                    .set({
                        status: 'delivered',
                        paidAt: new Date(),
                        deliveredAt: new Date(),
                        tradeNo: tradeNo,
                        cardKey: cardKey
                    })
                    .where(eq(orders.orderId, orderId));
                console.log(`[Fulfill] Order ${orderId} delivered successfully!`);
            } else {
                // Paid but no stock
                await tx.update(orders)
                    .set({ status: 'paid', paidAt: new Date(), tradeNo: tradeNo })
                    .where(eq(orders.orderId, orderId));
                console.log(`[Fulfill] Order ${orderId} marked as paid (no stock)`);
            }
        });
        return { success: true, status: 'processed' };
    } else {
        return { success: true, status: 'already_processed' }; // Idempotent success
    }
}
