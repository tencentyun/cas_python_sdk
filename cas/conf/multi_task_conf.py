# -*- coding=UTF-8 -*-

from multiprocessing import cpu_count

Default_Thread_Num = None      # 该项为None，则默认使用CPU的核心数*4
Job_NumberThread = Default_Thread_Num or (cpu_count() * 4)
Job_NumRetry = 3

MultipartUpload_NumberThread = Default_Thread_Num or (cpu_count() * 8)
MultipartUpload_NumberRetry = 3
