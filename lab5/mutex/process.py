import logging
import random
import time

from constMutex import ENTER, RELEASE, ALLOW, ACTIVE


class Process:
    """
    Implements access management to a critical section (CS) via fully
    distributed mutual exclusion (MUTEX).

    Processes broadcast messages (ENTER, ALLOW, RELEASE) timestamped with
    logical (lamport) clocks. All messages are stored in local queues sorted by
    logical clock time.

    Processes follow different behavioral patterns. An ACTIVE process competes 
    with others for accessing the critical section. A PASSIVE process will never 
    request to enter the critical section itself but will allow others to do so.

    A process broadcasts an ENTER request if it wants to enter the CS. A process
    that doesn't want to ENTER replies with an ALLOW broadcast. A process that
    wants to ENTER and receives another ENTER request replies with an ALLOW
    broadcast (which is then later in time than its own ENTER request).

    A process enters the CS if a) its ENTER message is first in the queue (it is
    the oldest pending message) AND b) all other processes have sent messages
    that are younger (either ENTER or ALLOW). RELEASE requests purge
    corresponding ENTER requests from the top of the local queues.

    Message Format:

    <Message>: (Timestamp, Process_ID, <Request_Type>)

    <Request Type>: ENTER | ALLOW  | RELEASE

    """

    def __init__(self, chan):
        self.channel = chan  # Create ref to actual channel
        self.process_id = self.channel.join('proc')  # Find out who you are
        self.all_processes: list = []  # All procs in the proc group
        self.other_processes: list = []  # Needed to multicast to others
        self.queue = []  # The request queue list
        self.clock = 0  # The current logical clock
        self.peer_name = 'unassigned'  # The original peer name
        self.peer_type = 'unassigned'  # A flag indicating behavior pattern
        self.logger = logging.getLogger("vs2lab.lab5.mutex.process.Process")

        # track which processes are alive
        self.alive_processes: set = set()
        self.timeout_counters: dict = {}  # consecutive timeouts per process
        self.max_allowed_timeouts = 5
        self.waiting_for_response: bool = False  # True when waiting for ALLOWs

    def __mapid(self, id='-1'):
        # format channel member address
        if id == '-1':
            id = self.process_id
        return 'Proc-'+str(id)

    def __detect_crashed_processes(self):
        """
        Detect crashed processes based on timeouts.
        Two scenarios:
        1. WE are at the head, waiting for ALLOWs - detect who hasn't responded
        2. Someone else is at head AND we have our own ENTER waiting - they might have crashed
        """
        if not self.queue:
            return
            
        head_of_queue = self.queue[0]
        
        # Scenario 1: WE are at the head, waiting for ALLOWs
        if head_of_queue[2] == ENTER and head_of_queue[1] == self.process_id and self.waiting_for_response:
            # Find who hasn't responded yet
            processes_that_responded = set([msg[1] for msg in self.queue[1:]])
            alive_others = [p for p in self.other_processes if p in self.alive_processes]
            
            for proc_id in alive_others:
                if proc_id not in processes_that_responded:
                    if proc_id not in self.timeout_counters:
                        self.timeout_counters[proc_id] = 0
                    
                    self.timeout_counters[proc_id] += 1
                    
                    if self.timeout_counters[proc_id] >= self.max_allowed_timeouts:
                        self.__mark_as_crashed(proc_id)
        
        # Scenario 2: Someone else is at the head (and we have an ENTER in queue)
        # They should be in CS and eventually RELEASE - if not, they crashed
        # Use a higher threshold since they might just be waiting for ALLOWs themselves
        elif head_of_queue[2] == ENTER and head_of_queue[1] != self.process_id:
            my_enter_in_queue = any(msg[1] == self.process_id and msg[2] == ENTER for msg in self.queue)
            if my_enter_in_queue:
                blocking_proc = head_of_queue[1]
                if blocking_proc in self.alive_processes:
                    if blocking_proc not in self.timeout_counters:
                        self.timeout_counters[blocking_proc] = 0
                    
                    self.timeout_counters[blocking_proc] += 1
                    
                    # Double the threshold for scenario 2 to give head time to detect crashes
                    if self.timeout_counters[blocking_proc] >= self.max_allowed_timeouts * 2:
                        self.__mark_as_crashed(blocking_proc)
    
    def __mark_as_crashed(self, proc_id):
        """Mark a process as crashed and clean up its messages."""
        self.alive_processes.discard(proc_id)
        
        print("\n*** [CRASH DETECTED] {} says: {} is DEAD! ***".format(
            self.__mapid(), self.__mapid(proc_id)), flush=True)
        print("    Remaining alive: {}\n".format(
            [self.__mapid(p) for p in sorted(self.alive_processes)]), flush=True)
        
        # Reset all timeout counters - situation has changed
        self.timeout_counters.clear()
        
        # Remove crashed process' messages from queue
        self.queue = [msg for msg in self.queue if msg[1] != proc_id]
        if self.queue:
            self.__cleanup_queue()
    
    def __cleanup_queue(self):
        if len(self.queue) > 0:
            # self.queue.sort(key = lambda tup: tup[0])
            self.queue.sort()
            # There should never be old ALLOW messages at the head of the queue
            while self.queue[0][2] == ALLOW:
                del (self.queue[0])
                if len(self.queue) == 0:
                    break

    def __request_to_enter(self):
        self.clock = self.clock + 1  # Increment clock value
        request_msg = (self.clock, self.process_id, ENTER)
        self.queue.append(request_msg)  # Append request to queue
        self.__cleanup_queue()  # Sort the queue
        self.waiting_for_response = True  # now waiting for ALLOWs
        self.channel.send_to(self.other_processes, request_msg)  # Send request

    def __allow_to_enter(self, requester):
        self.clock = self.clock + 1  # Increment clock value
        msg = (self.clock, self.process_id, ALLOW)
        self.channel.send_to([requester], msg)  # Permit other

    def __release(self):
        # need to be first in queue to issue a release
        assert self.queue[0][1] == self.process_id, 'State error: inconsistent local RELEASE'

        # construct new queue from later ENTER requests (removing all ALLOWS)
        tmp = [r for r in self.queue[1:] if r[2] == ENTER]
        self.queue = tmp  # and copy to new queue
        self.clock = self.clock + 1  # Increment clock value
        self.waiting_for_response = False  # no longer waiting for ALLOWs
        # reset timeout counters after successful CS access
        self.timeout_counters = {proc_id: 0 for proc_id in self.other_processes if proc_id in self.alive_processes}
        msg = (self.clock, self.process_id, RELEASE)
        # Multicast release notification
        self.channel.send_to(self.other_processes, msg)

    def __allowed_to_enter(self):
        # See who has sent a message (the set will hold at most one element per sender)
        processes_with_later_message = set([req[1] for req in self.queue[1:]])
        # Access granted if this process is first in queue and all __ALIVE__ others have answered (logically) later
        first_in_queue = self.queue[0][1] == self.process_id
        
        # only consider alive processes (excluding self)
        alive_others = [p for p in self.other_processes if p in self.alive_processes]
        all_alive_have_answered = len(alive_others) == len(processes_with_later_message)
        
        return first_in_queue and all_alive_have_answered

    def __receive(self):
        # Pick up any message
        _receive = self.channel.receive_from(self.other_processes, 3)
        if _receive:
            msg = _receive[1]
            sender_id = msg[1]
            
            # process is alive -> reset timeout counter for it
            if sender_id in self.timeout_counters:
                logging.debug("{} resetting timeout counter for {}.".format(
                    self.__mapid(), self.__mapid(sender_id)))
                self.timeout_counters[sender_id] = 0

            self.clock = max(self.clock, msg[0])  # Adjust clock value...
            self.clock = self.clock + 1  # ...and increment

            self.logger.debug("{} received {} from {}.".format(
                self.__mapid(),
                "ENTER" if msg[2] == ENTER
                else "ALLOW" if msg[2] == ALLOW
                else "RELEASE", self.__mapid(msg[1])))

            if msg[2] == ENTER:
                self.queue.append(msg)  # Append an ENTER request
                # and unconditionally allow (don't want to access CS oneself)
                self.__allow_to_enter(msg[1])
            elif msg[2] == ALLOW:
                self.queue.append(msg)  # Append an ALLOW
            elif msg[2] == RELEASE:
                # assure release requester indeed has access (his ENTER is first in queue)
                assert self.queue[0][1] == msg[1] and self.queue[0][2] == ENTER, 'State error: inconsistent remote RELEASE'
                del (self.queue[0])  # Just remove first message

            self.__cleanup_queue()  # Finally sort and cleanup the queue
        else:
            # timeout occurred -> check if any processes crashed
            self.__detect_crashed_processes()
            
            # Only log if we're actually waiting for something
            if self.waiting_for_response:
                self.logger.debug("{} timeout while waiting for responses".format(self.__mapid()))

    def init(self, peer_name, peer_type):
        self.channel.bind(self.process_id)

        self.all_processes = list(self.channel.subgroup('proc'))
        # sort string elements by numerical order
        self.all_processes.sort(key=lambda x: int(x))

        self.other_processes = list(self.channel.subgroup('proc'))
        self.other_processes.remove(self.process_id)
        
        # initialize all processes as alive
        self.alive_processes = set(self.all_processes)
        # initialize timeout counters
        self.timeout_counters = {proc_id: 0 for proc_id in self.other_processes}

        self.peer_name = peer_name  # assign peer name
        self.peer_type = peer_type  # assign peer behavior

        print("[INIT] {} = {} ({})".format(peer_name, self.__mapid(), peer_type), flush=True)

    def run(self):
        while True:
            # Enter the critical section if
            # 1) there are more than one process left and
            # 2) this peer has active behavior and
            # 3) random is true
            if len(self.all_processes) > 1 and \
                    self.peer_type == ACTIVE and \
                    random.choice([True, False]):
                self.logger.debug("{} wants to ENTER CS at CLOCK {}."
                                  .format(self.__mapid(), self.clock))

                self.__request_to_enter()
                while not self.__allowed_to_enter():
                    self.__receive()

                # Stay in CS for some time ...
                sleep_time = random.randint(0, 2000)
                self.logger.debug("{} enters CS for {} milliseconds."
                                  .format(self.__mapid(), sleep_time))
                print(" CS <- {}".format(self.__mapid()), flush=True)
                time.sleep(sleep_time/1000)

                # ... then leave CS
                print(" CS -> {}".format(self.__mapid()), flush=True)
                self.__release()
                continue

            # Occasionally serve requests to enter
            if random.choice([True, False]):
                self.__receive()
