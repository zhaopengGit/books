# Spark执行流程和脚本

![img](https://img-blog.csdn.net/2018062711193771?watermark/2/text/aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L09lbGpla2xhdXM=/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70)

过程描述:

1.通过Shell脚本启动Master，Master类继承Actor类，通过ActorySystem创建并启动。

2.通过Shell脚本启动Worker，Worker类继承Actor类，通过ActorySystem创建并启动。

3.Worker通过Akka或者Netty发送消息向Master注册并汇报自己的资源信息(内存以及CPU核数等)，以后就是定时汇报，保持心跳。

4.Master接受消息后保存(源码中通过持久化引擎持久化)并发送消息表示Worker注册成功，并且定时调度，移除超时的Worker。

5.通过Spark-Submit提交作业或者通过Spark Shell脚本连接集群，都会启动一个Spark进程Driver。

6.Master拿到作业后根据资源筛选Worker并与Worker通信，发送信息，主要包含Driver的地址等。

7.Worker进行收到消息后，启动Executor，Executor与Driver通信。

8.Driver端计算作业资源，transformation在Driver 端完成，划分各个Stage后提交Task给Executor。

9.Exectuor针对于每一个Task读取HDFS文件，然后计算结果，最后将计算的最终结果聚合到Driver端或者写入到持久化组件中。

# SparkContext流程

1.创建SparkEnv，里面有一个很重要的对象ActorSystem

2.创建TaskScheduler,这里是根据提交的集群来创建相应的TaskScheduler

3.对于TaskScheduler,主要的任务调度模式有FIFO和FAIR

4.在SparkContext中创建了两个Actor,一个是DriverActor,这里主要用于Driver和Executor之间的通信;还有一个是ClientActor,主要用于Driver和Master之间的通信。

5.创建DAGScheduler,其实这个是用于Stage的划分

6.调用taskScheduler.start()方法启动,进行资源调度,有两种资源分配方法,一种是尽量打散;一种是尽量集中

7.Driver向Master注册,发送了一些信息,其中一个重要的类是CoarseGrainedExecutorBackend,这个类以后用于创建Executor进程。

# Executor启动流程

1.Worker创建Executor进程,该进程的实现类其实是CoarseGrainedExecutorBackend

2.CoarseGrainedExecutorBackend向DriverActor注册成功后创建Executor对象,内部有一个可变的线程池

3.执行makeOffers()方法，查看是否有任务需要提交



# WordCount的Stage划分

stage划分算法如下:

涉及的数据结构:栈、HashSet

1.通过最后的RDD,获取父RDD

2.将finalRDD放入栈中,然后出栈,进行for循环的找到RDD的依赖,需要注意的是RDD可能有多个依赖

3.如果RDD依赖是ShuffleDependency,那么就可以划分成为一个新的Stage,然后通过getShuffleMapStage()获取这个stage的父stage;如果是一般的窄依赖,那么将会入栈

4.通过getShuffleMapStage()递归调用,得到父stage;一直到父stage是null

5.最后返回stage的集合

stage提交主要是通过递归算法,根据最后一个Stage划分然后递归找到第一个stage开始从第一个stage开始提交。

# Task提交流程

1.提交Task主要是迭代TaskSet一个一个的取出Task进行序列化之后向Executor发送序列化好的Task

2.Executor执行Task,创建一个TaskRunner对象将Task的信息封装信息然后使用线程池执行



# Spark的stage划分算法源码分析

DAGScheduler的stage划分算法总结:会从触发action操作的那个rdd开始反向解析,首先会为最后一个rdd创建一个stage,反向解析的时候，遇到窄依赖就把当前的rdd加入到Stage,遇到宽依赖就断开,将宽依赖的那个rdd创建一个新的stage，那个rdd就是这个stage最后一个rdd。依此类推,遍历所有RDD为止。



这里主要讲解的是stage的划分。stage的划分工作是在DAGScheduler中完成的，在DAGScheduler中会将一个job根据宽窄依赖划分为多个stage。下面会详细介绍stage的划分过程。



1、因为执行action算子的话，会提交任务到集群中，所以每个action算子底层都会调用一个runJob方法，来提交任务。

2、进入这个runJob方法，你会发现它底层调用的气势是SparkContext(spark上下)的runJob方法，这个方法需要传入三个参数

第一个参数：rdd

第二个参数：processPartition(在RDD的每个分区上运行的函数),例如调用tabke(2)这个2就相当于一个函数了。

3、点击runJob进入到DAGScheduler中的runJob方法，这里也会传入很多参数，这里就不一一介绍了。当进入到DAGScheduler中，这里会调用那个submitJob方法，去提交一个Job任务，然后会返回一个阻塞线程等待job的完成。

4、点击submitJob进入到这个方法中。在submitJob方法中，首先会检查传入的rdd分区是否存在，然后会为当前的这个job任务创建一个jobID，因为一个spark集群中可能会有多个job任务同时运行。如果发现partitions的长度为0了，也就是说不存在任务了，这里会就返回一个阻塞线程，使得runjob中的阻塞线程得以释放掉。

5、当分区的长度大于0的话，也就是说这个job中还存在任务可以执行，首先会创建一个阻塞的线程，eventProcessLoop是一个DAGScheduler的事件队列，因为一个spark集群中是可以存在多个job任务同时运行的，所以这里采用了FIFO先进先出的资源调度队列(还有一个任务调度队列是FAIR公平调度队列，它不是先进先出，而是根据集群的资源利用和job本身的情况进行一个友好的调度，这里可以自行百度查找)，

6、点击DAGSchedulerEventProcessLoop进入到这个类中，scala中有一个强大的模式匹配功能。当进入到这个类的时候会调用doOnReceive方法，使用模式匹配，确定调用那个类，方法，任务的提交的话是进入箭头所指的这个方法中

7、在介绍handleJobSubmitted之前，我想介绍一个什么是ResultStage和ShuffleMapStage，因为刚开始的时候我也是有点懵的，不懂这两个到底是个什么东东。

shuffle map stage
其实就是,非最终stage, 后面还有其他的stage, 所以它的输出一定是需要shuffle并作为后续的输入
result stage
最终的stage, 没有输出, 而是直接产生结果或存储 

当进入到handleJobSubmitted中，可以看到这里会创建一个finalStage这样的一个ResultStage,可能你会很好奇的问为什么起了这个名字，如果了解过stage划分原理的小伙伴应该都知道，气势stage的划分是从后往前推的，也就是说这里会先找到最后一个stage，然后通过依赖关系继续往前寻找stage(stage划分的一句是shuffle，如果就shuffle的话就会产生stage)，这里stage划分为shuffleMapStage和ResultStage两种，这里每个job是由1个ResultStage和0+个ShuffleMapStage组成。

8、点击createResultStage，进入看看这里的resultStage是如何创建的，这个方法主要是用来创建ResultStage。getOrCreateParentStages这个方法一会进去看看，他主要做的是通过shuffle之间的依赖关系去寻找是否有shuffle操作，如果有的话就会创建shuffleMapStage，如果遍历一遍都没有发现，就会返回一个list空的集合。这里的parents是一个list集合，里面存放都是ShuffleMapStage。

9、点击getorCreateParentStages这个方法，来一探究竟，父stage是怎么生成的。这里做的工作主要是抽取当前rdd的Shuffle的依赖，这里调用那个getOrcreateShuffleMapStage方法来创建shuffleMapStage，这里将传入两个参数，shuffleDep：是shuffle的依赖关系，firstJobId:job的id。
10、点击getOrCreateShuffleMapStage，这里会通过传递过来的shuffleDep，提取到shuffleId从而获取shuffleMapStage，如果能获取到shuffleMapStage的话，就直接返回stage，如果没有的就会循环遍历所有的shuffleDep，构建shuffleMapStage，具体请看createShuffleMapStage方法。

11、点击createShuffleMapStage,这里有有一段代码val numTasks = rdd.partitions.lenth，可以看出这里task的数量和partition(分区的数量是相同的)。

```php
  // stage划分总结
  // 1、从finalStage推断
  // 2、通过宽依赖,来进行新的stage的划分
  // 3、使用递归,优先提交父stage
```

因此spark划分stage的整体思路是：从后往前推，遇到宽依赖就断开，划分为一个stage；遇到窄依赖就将这个RDD加入该stage中。

　　在spark中，Task的类型分为2种：ShuffleMapTask和ResultTask；简单来说，DAG的最后一个阶段会为每个结果的partition生成一个ResultTask，即每个Stage里面的Task的数量是由该Stage中最后一个RDD的Partition的数量所决定的！

而其余所有阶段都会生成ShuffleMapTask；之所以称之为ShuffleMapTask是因为它需要将自己的计算结果通过shuffle到下一个stage中。
