# java-utils

## MPUtil
平常我们使用MP每次都要实现IService，ServiceImpl有没有觉得特别麻烦呢，虽然有代码生成器，但看着是不是也很不爽呀。如果你也觉得实现IService，ServiceImpl不够优雅，那么就来试试MPUtil吧。<br/>
- MP 3.5.0及以上版本
- 需要注册Mapper到Spring容器
- 纯静态工具类，摆脱 IService 和 ServiceImpl
