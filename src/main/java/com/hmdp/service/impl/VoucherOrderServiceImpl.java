package com.hmdp.service.impl;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;

import cn.hutool.core.bean.BeanUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
    implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    // 1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders > 
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 2. 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败, 说明没有消息, 继续下一次循环
                        continue;
                    }
                    
                    // 3. 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 3. 如果获取成功, 可以下单
                    handleVoucherOrder(voucherOrder);
                    // 4. ACK 确认 SACK stream.orders g1 id 
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                    
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1. 获取 pending-list 中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 2. 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败, 说明 pending-list 没有异常消息, 结束循环
                        break;
                    }

                    // 3. 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 3. 如果获取成功, 可以下单
                    handleVoucherOrder(voucherOrder);
                    // 4. ACK 确认 SACK stream.orders g1 id 
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理 pending-list 异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1 << 20);
//    private class VoucherOrderHandler implements Runnable {
//
//        @Override
//        public void run() {
//            while (true) {
//                // 1. 获取队列中的订单信息
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 2. 创建订单
//                    createVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单移除", e);
//                }
//            }
//        }
//    }

    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);

        // 尝试获取锁
        boolean isLock = redisLock.tryLock();

        // 判断
        if (!isLock) {
            // 获取锁失败, 直接返回失败 或者 重试
            log.error("不允许重复下单!");
            return;
        }

        try {
            // 5.1 查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();

            // 5.2 判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单!");
                return;
            }

            // 6. 扣减库存
            boolean success = seckillVoucherService.update().setSql("stock = stock - 1").eq("voucher_id", voucherId)
                .gt("stock", 0).update();

            if (!success) {
                // 扣减失败
                log.error("库存不足!");
                return;
            }
            
            save(voucherOrder);
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }
    
    private IVoucherOrderService proxy;
    
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1. 获取用户
        Long userId = voucherOrder.getUserId();
        // 2. 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 3. 获取锁
        boolean isLock = lock.tryLock();
        // 4. 判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败, 返回错误或重试
            log.error("不允许重复下单!");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
    
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 获取订单 id
        long orderId = redisIdWorker.nextId("order");
        // 1. 执行 lua 脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        // 2. 判断结果是否为 0
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为 0, 代表没有购买资格
            return Result.fail(r == 1 ? "库存不足!" : "不能重复下单!");
        }
        // 3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4. 返回订单 id
        return Result.ok(orderId);
    }
    

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        // 1. 执行 lua 脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        int r = result.intValue();
//
//        // 2. 判断结果是否为 0
//        if (r != 0) {
//            // 2.1 不为 0, 代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足!" : "不能重复下单!");
//        }
//        // 2.2 为0, 有购买资格, 把下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 2.3 订单 id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 2.4 用户 id
//        voucherOrder.setUserId(userId);
//        // 2.5 代金券 id
//        voucherOrder.setVoucherId(voucherId);
//        
//        // 2.6 放入阻塞队列
//        orderTasks.add(voucherOrder);
//        
//        // 3. 返回订单 id
//        return Result.ok(orderId);
//    }

    //    @Override
//    public Result seckillVoucher(Long voucherId) {
//
//        // 1. 查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        // 2. 判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀尚未开始!");
//        }
//
//        // 3. 判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            // 已经结束
//            return Result.fail("秒杀已经结束!");
//        }
//
//        // 4. 判断库存是否充足
//        if (voucher.getStock() < 1) {
//            // 库存不足
//            return Result.fail("库存不足!");
//        }
//
//        return createVoucherOrder(voucherId);
//    }

    // @Resource
    // private StringRedisTemplate stringRedisTemplate;
    
    
//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        // 5. 一人一单
//        Long userId = UserHolder.getUser().getId();
//
//        // 创建锁对象
//        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
//
//        // 尝试获取锁
//        boolean isLock = redisLock.tryLock();
//
//        // 判断
//        if (!isLock) {
//            // 获取锁失败, 直接返回失败 或者 重试
//            return Result.fail("不允许重复下单!");
//        }
//
//        try {
//            // 5.1 查询订单
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//
//            // 5.2 判断是否存在
//            if (count > 0) {
//                // 用户已经购买过了
//                return Result.fail("用户已经购买过一次!");
//            }
//
//            // 6. 扣减库存
//            boolean success = seckillVoucherService.update().setSql("stock = stock - 1").eq("voucher_id", voucherId)
//                .gt("stock", 0).update();
//
//            if (!success) {
//                // 扣减失败
//                return Result.fail("库存不足!");
//            }
//            // 7. 创建订单
//            VoucherOrder voucherOrder = new VoucherOrder();
//            // 7.1 订单 id
//            long orderId = redisIdWorker.nextId("order");
//            voucherOrder.setId(orderId);
//            // 7.2 用户 id
//            voucherOrder.setUserId(userId);
//            // 7.3 代金券 id
//            voucherOrder.setVoucherId(voucherId);
//
//            save(voucherOrder);
//
//            // 8. 返回订单 id
//            return Result.ok(voucherId);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            // 释放锁
//            redisLock.unlock();
//        }
//    }

    // /**
    // * @author wendong
    // * @date 2022/5/17
    // * @methodName createVoucherOrder
    // * @description redis 分布式锁版本 1
    // * @param voucherId
    // * @return com.hmdp.dto.Result
    // *
    // **/
    // @Transactional
    // public Result createVoucherOrder(Long voucherId) {
    // // 5. 一人一单
    // Long userId = UserHolder.getUser().getId();
    //
    // // 创建锁对象
    // SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
    //
    // // 尝试获取锁
    // boolean isLock = redisLock.tryLock(1200);
    //
    // // 判断
    // if (!isLock) {
    // // 获取锁失败, 直接返回失败 或者 重试
    // return Result.fail("不允许重复下单!");
    // }
    //
    // try {
    // // 5.1 查询订单
    // int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
    //
    // // 5.2 判断是否存在
    // if (count > 0) {
    // // 用户已经购买过了
    // return Result.fail("用户已经购买过一次!");
    // }
    //
    // // 6. 扣减库存
    // boolean success = seckillVoucherService.update().setSql("stock = stock - 1").eq("voucher_id", voucherId)
    // .gt("stock", 0).update();
    //
    // if (!success) {
    // // 扣减失败
    // return Result.fail("库存不足!");
    // }
    // // 7. 创建订单
    // VoucherOrder voucherOrder = new VoucherOrder();
    // // 7.1 订单 id
    // long orderId = redisIdWorker.nextId("order");
    // voucherOrder.setId(orderId);
    // // 7.2 用户 id
    // voucherOrder.setUserId(userId);
    // // 7.3 代金券 id
    // voucherOrder.setVoucherId(voucherId);
    //
    // save(voucherOrder);
    //
    // // 8. 返回订单 id
    // return Result.ok(voucherId);
    // } catch (Exception e) {
    // throw new RuntimeException(e);
    // } finally {
    // // 释放锁
    // redisLock.unlock();
    // }
    // }

    // /**
    // * @author wendong
    // * @date 2022/5/17
    // * @methodName createVoucherOrder
    // * @description 以用户id加悲观锁, 实现一人一单
    // * @param voucherId
    // * @return com.hmdp.dto.Result
    // *
    // **/
    // @Transactional
    // public Result createVoucherOrder(Long voucherId) {
    // // 5. 一人一单
    // Long userId = UserHolder.getUser().getId();
    //
    // synchronized (userId.toString().intern()) {
    // // 5.1 查询订单
    // int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
    //
    // // 5.2 判断是否存在
    // if (count > 0) {
    // // 用户已经购买过了
    // return Result.fail("用户已经购买过一次!");
    // }
    //
    // // 6. 扣减库存
    // boolean success = seckillVoucherService.update()
    // .setSql("stock = stock - 1")
    // .eq("voucher_id", voucherId)
    // .gt("stock", 0)
    // .update();
    //
    // if (!success) {
    // // 扣减失败
    // return Result.fail("库存不足!");
    // }
    // // 7. 创建订单
    // VoucherOrder voucherOrder = new VoucherOrder();
    // // 7.1 订单 id
    // long orderId = redisIdWorker.nextId("order");
    // voucherOrder.setId(orderId);
    // // 7.2 用户 id
    // voucherOrder.setUserId(userId);
    // // 7.3 代金券 id
    // voucherOrder.setVoucherId(voucherId);
    //
    // save(voucherOrder);
    //
    // // 8. 返回订单 id
    //
    // return Result.ok(voucherId);
    // }
}
