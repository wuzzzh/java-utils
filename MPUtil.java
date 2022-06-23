package com.wuzzzh.template.springboot.util;

import cn.hutool.core.lang.Assert;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.enums.SqlMethod;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.baomidou.mybatisplus.core.toolkit.*;
import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.conditions.update.LambdaUpdateChainWrapper;
import com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper;
import com.baomidou.mybatisplus.extension.toolkit.SqlHelper;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionUtils;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 封装Mybatis Plus CRUD操作
 * 限制：
 * - 需要注册Mapper到Spring容器
 * 特点：
 * - 纯静态工具类，摆脱 IService 和 ServiceImpl
 *
 * @author wuzzzh
 */
@Slf4j
@UtilityClass
public class MPUtil {
    /**
     * 默认批次提交数量
     */
    private static final int DEFAULT_BATCH_SIZE = 1000;


    /**
     * 查询单个字段的值
     * - 用法：listObjs(Wrappers.lambdaQuery(User.class).select(User::getId), o -> (Integer) o)
     *
     * @param queryWrapper 查询条件
     * @param mapper       字段映射规则
     */
    public static <T, V> List<V> listObjs(LambdaQueryWrapper<T> queryWrapper, Function<? super Object, V> mapper) {
        return execute(queryWrapper.getEntityClass(), baseMapper -> baseMapper.selectObjs(queryWrapper)
                .stream()
                .filter(Objects::nonNull)
                .map(mapper)
                .collect(Collectors.toList())
        );
    }

    /**
     * 查询多个字段的值
     * - 用法：listObjs(Wrappers.lambdaQuery(User.class).select(User::getId, User::getName))
     *
     * @param queryWrapper 查询条件
     */
    public static <T> List<Map<String, Object>> listMaps(LambdaQueryWrapper<T> queryWrapper) {
        return execute(queryWrapper.getEntityClass(), baseMapper -> baseMapper.selectMaps(queryWrapper));
    }

    /**
     * 链式查询
     * - 用法：query(User.class).eq("`name`", "lisi").list();
     *
     * @param entityClass 表实体类型
     */
    public static <T> QueryChainWrapper<T> query(Class<T> entityClass) {
        return new QueryChainWrapper<T>(getMapper(entityClass, SqlHelper.sqlSession(entityClass))) {
            @Override
            public List<T> list() {
                return execute(entityClass, baseMapper -> baseMapper.selectList(getWrapper()));
            }

            @Override
            public T one() {
                return execute(entityClass, baseMapper -> baseMapper.selectOne(getWrapper()));
            }

            @Override
            public Optional<T> oneOpt() {
                return Optional.ofNullable(one());
            }

            @Override
            public Long count() {
                return execute(entityClass, baseMapper -> baseMapper.selectCount(getWrapper()));
            }

            @Override
            public boolean exists() {
                return execute(entityClass, baseMapper -> baseMapper.exists(getWrapper()));
            }

            @Override
            public <E extends IPage<T>> E page(E page) {
                return execute(entityClass, baseMapper -> baseMapper.selectPage(page, getWrapper()));
            }

        };
    }

    /**
     * lambda链式查询
     * - 用法：lambdaQuery(User.class).eq(User::getName, "lisi").list();
     *
     * @param entityClass 表实体类型
     */
    public static <T> LambdaQueryChainWrapper<T> lambdaQuery(Class<T> entityClass) {
        return new LambdaQueryChainWrapper<T>(getMapper(entityClass, SqlHelper.sqlSession(entityClass))) {

            @Override
            public List<T> list() {
                return execute(entityClass, baseMapper -> baseMapper.selectList(getWrapper()));
            }

            @Override
            public T one() {
                return execute(entityClass, baseMapper -> baseMapper.selectOne(getWrapper()));
            }

            @Override
            public Optional<T> oneOpt() {
                return Optional.ofNullable(one());
            }

            @Override
            public Long count() {
                return execute(entityClass, baseMapper -> baseMapper.selectCount(getWrapper()));
            }

            @Override
            public boolean exists() {
                return execute(entityClass, baseMapper -> baseMapper.exists(getWrapper()));
            }

            @Override
            public <E extends IPage<T>> E page(E page) {
                return execute(entityClass, baseMapper -> baseMapper.selectPage(page, getWrapper()));
            }


        };
    }


    /**
     * 链式更改
     * - 用法：update(User.class).set("`name`", "zhangsan").eq("`name`", "lisi").update();
     *
     * @param entityClass 表实体类型
     */
    public static <T> UpdateChainWrapper<T> update(Class<T> entityClass) {
        return new UpdateChainWrapper<T>(getMapper(entityClass, SqlHelper.sqlSession(entityClass))) {
            @Override
            public boolean update() {
                return update(null);
            }

            @Override
            public boolean update(T entity) {
                return execute(entityClass, baseMapper -> SqlHelper.retBool(baseMapper.update(entity, getWrapper())));
            }

            @Override
            public boolean remove() {
                return execute(entityClass, baseMapper -> SqlHelper.retBool(baseMapper.delete(getWrapper())));
            }
        };
    }

    /**
     * lambda链式更改
     * - 用法：lambdaUpdate(User.class).set(User::getName, "zhangsan").eq(User::getName, "lisi").update();
     *
     * @param entityClass 表实体类型
     */
    public static <T> LambdaUpdateChainWrapper<T> lambdaUpdate(Class<T> entityClass) {
        return new LambdaUpdateChainWrapper<T>(getMapper(entityClass, SqlHelper.sqlSession(entityClass))) {
            @Override
            public boolean update() {
                return update(null);
            }

            @Override
            public boolean update(T entity) {
                return execute(entityClass, baseMapper -> SqlHelper.retBool(baseMapper.update(entity, getWrapper())));
            }

            @Override
            public boolean remove() {
                return execute(entityClass, baseMapper -> SqlHelper.retBool(baseMapper.delete(getWrapper())));
            }
        };
    }

    /**
     * 根据主键批量查询
     *
     * @param entityClass 表实体类型
     * @param idList      主键ID集合
     */
    public static <T> List<T> listByIds(Class<T> entityClass, Collection<? extends Serializable> idList) {
        return execute(entityClass, baseMapper -> baseMapper.selectBatchIds(idList));
    }

    /**
     * 根据主键查询
     *
     * @param entityClass 表实体类型
     * @param id          主键ID
     */
    public static <T> T getById(Class<T> entityClass, Serializable id) {
        return execute(entityClass, baseMapper -> baseMapper.selectById(id));
    }

    /**
     * 根据主键批量删除
     *
     * @param entityClass 表实体类型
     * @param ids         主键ID
     */
    public static <T> boolean removeByIds(Class<T> entityClass, Collection<? extends Serializable> ids) {
        return execute(entityClass, baseMapper -> SqlHelper.retBool(baseMapper.deleteBatchIds(ids)));
    }

    /**
     * 根据主键删除
     *
     * @param entityClass 表实体类型
     * @param id          主键ID
     */
    public static <T> boolean removeById(Class<T> entityClass, Serializable id) {
        return execute(entityClass, baseMapper -> baseMapper.deleteById(id) == 1);
    }

    /**
     * 新增或修改
     * - 新增 - id is null
     * - 修改 - id is not null
     *
     * @param entity 表实体
     */
    @SuppressWarnings("all")
    public static <T> boolean saveOrUpdate(T entity) {
        if (getIdVal(entity) == null) {
            return save(entity);
        } else {
            return updateById(entity);
        }
    }

    /**
     * 根据主键批量更新
     *
     * @param entityList 表实体集合
     */
    public static <T> boolean updateBatchById(Collection<T> entityList) {
        return updateBatchById(entityList, DEFAULT_BATCH_SIZE);
    }

    /**
     * 根据主键批量更新
     *
     * @param entityList 表实体集合
     * @param batchSize  批次数量
     */
    public static <T> boolean updateBatchById(Collection<T> entityList, int batchSize) {
        Class<T> entityClass = getEntityClass(entityList);
        TableInfo tableInfo = getTableInfo(entityClass);
        String sqlStatement = SqlHelper.getSqlStatement(ClassUtils.toClassConfident(tableInfo.getCurrentNamespace()), SqlMethod.UPDATE_BY_ID);
        return SqlHelper.executeBatch(entityClass, LogFactory.getLog(MPUtil.class), entityList, batchSize, (sqlSession, entity) -> {
            MapperMethod.ParamMap<T> param = new MapperMethod.ParamMap<>();
            param.put(Constants.ENTITY, entity);
            sqlSession.update(sqlStatement, param);
        });
    }

    /**
     * 根据主键修改
     *
     * @param entity 表实体
     */
    @SuppressWarnings("all")
    public static <T> boolean updateById(T entity) {
        Serializable idVal = getIdVal(entity);
        Assert.notNull(idVal);
        Class<T> entityClass = (Class<T>) entity.getClass();
        T obj = execute(entityClass, baseMapper -> baseMapper.selectById(idVal));
        if (obj == null) {
            log.warn("[ MPUtil#updateById ] Class={}, id={} is not exists", entityClass.getSimpleName(), idVal);
            return false;
        }
        return execute(entityClass, baseMapper -> baseMapper.updateById(entity) == 1);
    }

    /**
     * 批量新增
     *
     * @param entityList 表实体集合
     */
    public static <T> void saveBatch(Collection<T> entityList) {
        saveBatch(entityList, DEFAULT_BATCH_SIZE);
    }

    /**
     * 批量新增
     *
     * @param entityList 表实体集合
     * @param batchSize  批次数量
     */
    @SuppressWarnings("all")
    public static <T> boolean saveBatch(Collection<T> entityList, int batchSize) {
        Class<T> entityClass = getEntityClass(entityList);
        Class<?> mapperClass = ClassUtils.toClassConfident(getTableInfo(entityClass).getCurrentNamespace());
        String sqlStatement = SqlHelper.getSqlStatement(mapperClass, SqlMethod.INSERT_ONE);
        return SqlHelper.executeBatch(entityClass, LogFactory.getLog(MPUtil.class), entityList, batchSize, (sqlSession, entity) -> sqlSession.insert(sqlStatement, entity));
    }

    /**
     * 新增
     *
     * @param entity 表实体
     */
    @SuppressWarnings("all")
    public static <T> boolean save(T entity) {
        Assert.notNull(entity);
        Class<T> entityClass = (Class<T>) entity.getClass();
        return execute(entityClass, baseMapper -> baseMapper.insert(entity) == 1);
    }

    /**
     * 字段重复性校验
     * - 通过对期望count和实际count的比对来确定是否重复
     * - 如 新增时expectCount=0,修改时expectCount=1
     *
     * @param id            主键ID 通过其是否为 null 来赋值 expectCount
     * @param checkConsumer 自定义校验逻辑
     */
    public static void repeatCheck(Serializable id, Consumer<Integer> checkConsumer) {
        int expectCount = id == null ? 0 : 1;
        checkConsumer.accept(expectCount);
    }

    /**
     * 通过entityClass获取BaseMapper，执行 Function
     *
     * @param entityClass 实体类型
     * @param sFunction   lambda操作
     * @return 返回lambda执行结果
     */
    private static <T, R> R execute(Class<T> entityClass, SFunction<BaseMapper<T>, R> sFunction) {
        SqlSession sqlSession = SqlHelper.sqlSession(entityClass);
        try {
            BaseMapper<T> baseMapper = getMapper(entityClass, sqlSession);
            return sFunction.apply(baseMapper);
        } finally {
            SqlSessionUtils.closeSqlSession(sqlSession, GlobalConfigUtils.currentSessionFactory(entityClass));
        }
    }

    /**
     * 通过entityClass获取Mapper，记得要释放连接
     * 例： {@code
     * SqlSession sqlSession = SqlHelper.sqlSession(entityClass);
     * try {
     *  BaseMapper<User> userMapper = getMapper(User.class, sqlSession);
     * } finally {
     *  sqlSession.close();
     * }
     * }
     *
     * @param entityClass 表实体类型
     * @param sqlSession SqlHelper.sqlSession(entityClass)
     * @return BaseMapper
     */
    @SuppressWarnings("unchecked")
    private static <T> BaseMapper<T> getMapper(Class<T> entityClass, SqlSession sqlSession) {
        com.baomidou.mybatisplus.core.toolkit.Assert.notNull(entityClass, "entityClass can't be null!");
        TableInfo tableInfo = Optional.ofNullable(TableInfoHelper.getTableInfo(entityClass)).orElseThrow(() -> ExceptionUtils.mpe("Can not find TableInfo from Class: \"%s\".", entityClass.getName()));
        Class<?> mapperClass = ClassUtils.toClassConfident(tableInfo.getCurrentNamespace());
        return (BaseMapper<T>) tableInfo.getConfiguration().getMapper(mapperClass, sqlSession);
    }

    /**
     * 获取表实体主键值
     */
    private static <T> Serializable getIdVal(T entity) {
        Assert.notNull(entity);
        TableInfo tableInfo = getTableInfo(entity.getClass());
        String keyProperty = tableInfo.getKeyProperty();
        Assert.notBlank(keyProperty);
        return (Serializable) ReflectionKit.getFieldValue(entity, keyProperty);
    }

    /**
     * 从集合中获取表实体类型
     *
     * @param entityList 实体集合
     * @return 表实体类型
     */
    @SuppressWarnings("unchecked")
    private static <T> Class<T> getEntityClass(Collection<T> entityList) {
        Assert.notEmpty(entityList);
        return (Class<T>) entityList.iterator().next().getClass();
    }

    /**
     * 获取实体表信息，获取不到报错提示
     *
     * @param entityClass 表实体类型
     * @return 实体表信息
     */
    private static <T> TableInfo getTableInfo(Class<T> entityClass) {
        return Optional.ofNullable(TableInfoHelper.getTableInfo(entityClass)).orElseThrow(() -> ExceptionUtils.mpe("error: can not find TableInfo from Class: \"%s\".", entityClass.getName()));
    }

}
