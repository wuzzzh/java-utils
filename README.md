# java-utils

## MPUtil
平常我们使用MP每次都要实现IService，ServiceImpl特别麻烦，虽然有代码生成器，但看着是不是很不爽。如果你也觉得实现IService，ServiceImpl不优雅，那么就来试试MPUtil吧。<br/>
封装Mybatis Plus CRUD操作<br/>
限制：<br/>
- MP 3.5.0及以上版本
- 需要注册Mapper到Spring容器
<br/>
特点：<br/>
- 纯静态工具类，摆脱 IService 和 ServiceImpl
