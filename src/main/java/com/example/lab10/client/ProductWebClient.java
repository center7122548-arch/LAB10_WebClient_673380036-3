package com.example.lab10.client;

import com.example.lab10.model.Product;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * ProductWebClient — Reactive HTTP Client
 *
 * ✅ WebClient setup และ Bean config ครบแล้ว
 * ✅ method getProductById() ทำเสร็จแล้วเป็นตัวอย่าง (30%)
 * ❌ TODO: เติม method body ที่เหลือ (70%)
 *
 * WebClient method chain:
 *   client.get()              ← HTTP method
 *     .uri("/products/{id}", id)  ← URL
 *     .retrieve()             ← เริ่มรับ response
 *     .bodyToMono(T.class)    ← แปลงเป็น Mono<T>
 *     .bodyToFlux(T.class)    ← แปลงเป็น Flux<T>
 */
@Component
public class ProductWebClient {

    // ✅ WebClient ตั้งค่าแล้ว ชี้ไปที่ server ตัวเอง
    private final WebClient client = WebClient.create("http://localhost:8080");

    // ══════════════════════════════════════════════════════
    // ✅ ตัวอย่างที่ทำเสร็จแล้ว — ศึกษาแล้วทำ method ที่เหลือ
    // ══════════════════════════════════════════════════════

    /**
     * GET /products/{id} → Mono<Product>
     * ดึง Product 1 รายการจาก server
     */
    public Mono<Product> getProductById(String id) {
        return client.get()
                .uri("/products/{id}", id)
                .retrieve()
                .bodyToMono(Product.class);
    }

    // ══════════════════════════════════════════════════════
    // ❌ TODO: เติม method body ด้านล่างนี้
    // ══════════════════════════════════════════════════════

    /**
     * GET /products → Flux<Product>
     * TODO: ดึง Product ทั้งหมดจาก server
     *
     * Hint: client.get()
     *         .uri("/products")
     *         .retrieve()
     *         .bodyToFlux(Product.class)
     */
    public Flux<Product> getAllProducts() {
        return client.get()
                .uri("/products")
                .retrieve()
                .bodyToFlux(Product.class);
    }

    /**
     * POST /products → Mono<Product>
     * ส่ง Product ใหม่ไปยัง server ด้วย .bodyValue()
     * .bodyValue() ส่ง object เป็น JSON body โดยอัตโนมัติ
     * คืน Mono<Product> จาก response body (non-blocking)
     */
    public Mono<Product> createProduct(Product product) {
        return client.post()
                .uri("/products")
                .bodyValue(product)
                .retrieve()
                .bodyToMono(Product.class);
    }

    /**
     * DELETE /products/{id} → Mono<Void>
     * ส่ง DELETE request แล้วคืน Mono<Void>
     * Mono<Void> = completion signal ว่า server ลบสำเร็จ
     */
    public Mono<Void> deleteProduct(String id) {
        return client.delete()
                .uri("/products/{id}", id)
                .retrieve()
                .bodyToMono(Void.class);
    }

    /**
     * GET /products/category/{category} → Flux<Product>
     * ดึง Product ตาม category ด้วย bodyToFlux()
     * Flux ส่ง element ออกทีละรายการ non-blocking (Reactive Streams backpressure)
     */
    public Flux<Product> getByCategory(String category) {
        return client.get()
                .uri("/products/category/{category}", category)
                .retrieve()
                .bodyToFlux(Product.class);
    }

    /**
     * GET /products/{id}/price → Mono<Double>
     * ดึงราคาหลังส่วนลด แล้ว chain .doOnNext() เพื่อ log ราคา
     * .doOnNext() = side-effect operator ไม่เปลี่ยนแปลงค่า
     * ตามด้วย .map() เพื่อแปลงค่า และ .defaultIfEmpty() เป็น fallback
     */
    public Mono<Double> getDiscountedPrice(String id) {
        return client.get()
                .uri("/products/{id}/price", id)
                .retrieve()
                .bodyToMono(Double.class)
                .doOnNext(price -> System.out.println(
                        "[WebClient] Discounted price for id=" + id + " → " + price))
                .map(price -> price)
                .defaultIfEmpty(0.0);
    }
}

