package com.example.lab10.service;

import com.example.lab10.model.Product;
import com.example.lab10.repository.ProductRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * ProductService — Business Logic Layer
 *
 * ✅ @Service, Constructor Injection ครบแล้ว (DIP — SOLID)
 * ❌ TODO: เติม method body ให้ครบทุก method
 *
 * หน้าที่: รับ request จาก Controller → เรียก Repository → คืนผล
 * (SRP — แต่ละ class มีหน้าที่เดียว)
 *
 * Hint Operators ที่ควรใช้:
 *   .map(p -> ...)            แปลงค่า
 *   .flatMap(p -> ...)        async transform
 *   .defaultIfEmpty(...)      fallback ถ้าว่าง
 *   .switchIfEmpty(Mono...)   fallback Mono ถ้าว่าง
 */
@Service
public class ProductService {

    // ── Constructor Injection (DIP — SOLID) ─────────────
    private final ProductRepository repository;

    public ProductService(ProductRepository repository) {
        this.repository = repository;
    }

    // ── 1. ดึง Product 1 รายการ ──────────────────────────
    /**
     * TODO: เรียก repository.findById(id) แล้วคืนผล
     *       ถ้าไม่พบให้ throw RuntimeException("Product not found: " + id)
     *
     * Hint: repository.findById(id)
     *       .switchIfEmpty(Mono.error(new RuntimeException(...)))
     */
    public Mono<Product> getById(String id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new RuntimeException("Product not found: " + id)));
    }

    // ── 2. ดึง Product ทั้งหมด ───────────────────────────
    /**
     * เรียก repository.findAll() แล้วคืนผลเป็น Flux<Product>
     * ทุก element ถูกส่งออกแบบ non-blocking (Reactive Streams)
     */
    public Flux<Product> getAll() {
        return repository.findAll();
    }

    // ── 3. บันทึก Product ────────────────────────────────
    /**
     * ถ้า id เป็น null → generate UUID ใหม่ด้วย UUID.randomUUID()
     * จากนั้นใช้ flatMap เพื่อ chain การ save แบบ async
     */
    public Mono<Product> save(Product product) {
        if (product.getId() == null || product.getId().isBlank()) {
            product.setId(java.util.UUID.randomUUID().toString());
        }
        return repository.save(product);
    }

    // ── 4. ลบ Product ────────────────────────────────────
    /**
     * เรียก repository.deleteById(id) แล้วคืน Mono<Void>
     * Mono<Void> = signal ว่าสำเร็จโดยไม่มีค่า (complete event)
     */
    public Mono<Void> delete(String id) {
        return repository.deleteById(id);
    }

    // ── 5. กรองตาม category ──────────────────────────────
    /**
     * เรียก repository.findByCategory(category) ซึ่งใช้ .filter() ภายใน
     * คืน Flux<Product> ที่ถูกกรองแบบ non-blocking
     */
    public Flux<Product> getByCategory(String category) {
        return repository.findByCategory(category);
    }

    // ── 6. คำนวณราคาหลังส่วนลด ───────────────────────────
    /**
     * ใช้ .map() operator แปลง Product → Double (discounted price)
     * .map() เป็น synchronous transform ใน reactive pipeline
     * ไม่มี blocking: ทุกอย่างเกิดใน event loop thread
     */
    public Mono<Double> getDiscountedPrice(String id) {
        return getById(id)
                .map(p -> p.getDiscountedPrice());
    }
}

