docker run -it \
-v /home/disk3/xujia/.m2:/home/xujia/.m2 \
-v /home/disk3/xujia/WorkSpace/exp/starrocks:/home/xujia/starrocks \
-v /home/disk3/xujia:/home/xujia \
--user 1005:1005 \
--name xujia-sr-build-ubuntu-exp \
-d mxr-starrocks-dev:latest 


#--name xujia-sr-build-centos \
#-d 172.26.92.142:5000/starrocks/dev-env-centos7:latest












#--name xujia-debug-56192 \
#-d registry.ap-southeast-1.aliyuncs.com/starrocks/dev-env-ubuntu:main-56192 









