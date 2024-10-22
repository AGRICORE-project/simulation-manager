import time
from typing import Dict, List
from agricore_sp_models.simulation_models import LogLevel


class DelayedLog:
    """
    A logging utility that delays logging messages until a specified retention time or size is reached.

    Attributes:
        logger (Logger): The logger instance to which messages will be sent.
        retention_time (float): The time in seconds to retain messages before logging them.
        retention_size (int): The number of messages to retain before logging them.
        start_time (float): The time when the logging started or was last flushed.
        message_queue (Dict[LogLevel, List[str]]): A dictionary holding messages categorized by their log level.
    """
    def __init__ (self, logger, retention_time, retention_size) -> 'DelayedLog':
        """
        Initializes the DelayedLog object.

        Parameters:
            logger (Logger): The logger instance to which messages will be sent.
            retention_time (float): The time in seconds to retain messages before logging them.
            retention_size (int): The number of messages to retain before logging them.
        """
        self.logger = logger
        self.retention_time = retention_time
        self.retention_size = retention_size
        self.start_time = time.time()
        self.message_queue:Dict[LogLevel, List[str]] = {}

    def __enter__(self) -> 'DelayedLog':
        """
        Enters the runtime context related to this object.

        Returns:
            DelayedLog: The DelayedLog instance itself.
        """
        #ttysetattr etc goes here before opening and returning the file object
        return self
    
    def addMessage(self, message:str, level:LogLevel):
        """
        Adds a message to the queue with the specified log level.

        Parameters:
            message (str): The message to log.
            level (LogLevel): The level of the log message.
        """
        if level not in self.message_queue:
            self.message_queue[level] = []
        self.message_queue[level].append(message)
        self.check_queue_and_send()
        
    def check_queue_and_send(self, force = False):
        """
        Checks the message queue and sends messages to the logger if conditions are met.

        Parameters:
            force (bool): Forces the sending of messages regardless of retention time or size.
        """
        flush_queue = False
        time_now = time.time()
        elapsed_time = time_now - self.start_time

        if elapsed_time > self.retention_time:
            flush_queue = True
            
        num_messages = 0
        for s in self.message_queue:
            num_messages += len(self.message_queue[s])
        if num_messages > self.retention_size:
            flush_queue = True
            
        if force:
            flush_queue = True
        
        if flush_queue:
            for s in self.message_queue:
                msg_list = "\n".join(self.message_queue[s])
                if s == LogLevel.ERROR:
                    self.logger.error(msg_list)
                elif s == LogLevel.DEBUG:
                    self.logger.debug(msg_list)
                elif s == LogLevel.INFO:
                    self.logger.info(msg_list)
                elif s == LogLevel.SUCCESS:
                    self.logger.success(msg_list)
                elif s == LogLevel.WARNING:
                    self.logger.warning(msg_list)
            self.message_queue = {}
            self.start_time = time_now
            
    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Exits the runtime context related to this object, flushing the log queue.

        Parameters:
            exception_type (Type[BaseException]): The exception type.
            exception_value (BaseException): The exception value.
            exception_traceback (TracebackType): The traceback object.
        """
        #Exception handling here
        self.check_queue_and_send(force =True)
        if exception_value is not None:
            raise exception_type(exception_value, exception_traceback)
            