package mr

import (
	"log"
	"time"
)

type TaskType int
type TaskStatus int

const TaskTimeout time.Duration = 10

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	WaitTask
)

const (
	Waiting TaskStatus = iota
	Working
	Done
)

type TaskSet struct {
	mapTaskMap    map[int]*TaskMetaData
	reduceTaskMap map[int]*TaskMetaData
}

type TaskMetaData struct {
	Status    TaskStatus
	StartTime time.Time
	Task      *Task
}

type Task struct {
	Type           TaskType
	TaskId         int
	ReducerTaskNum int
	Files          []string
}

func NewTaskSet() *TaskSet {
	return &TaskSet{
		mapTaskMap:    map[int]*TaskMetaData{},
		reduceTaskMap: map[int]*TaskMetaData{},
	}
}

func NewTaskMetaData(task *Task) *TaskMetaData {
	return &TaskMetaData{
		Status: Waiting,
		Task:   task,
	}
}

func NewTask(taskType TaskType, taskId int, nReduce int, files []string) *Task {
	return &Task{
		Type:           taskType,
		TaskId:         taskId,
		ReducerTaskNum: nReduce,
		Files:          files,
	}
}

func (ts *TaskSet) RegisterTask(task *Task) {
	taskMetaData := NewTaskMetaData(task)
	switch task.Type {
	case MapTask:
		ts.mapTaskMap[task.TaskId] = taskMetaData
	case ReduceTask:
		ts.reduceTaskMap[task.TaskId] = taskMetaData
	default:
		log.Panic("Cannot add unsupported task to TaskSet.")
	}
}

func (ts *TaskSet) StartTask(task *Task) {
	var taskMetaData *TaskMetaData
	switch task.Type {
	case MapTask:
		taskMetaData = ts.mapTaskMap[task.TaskId]
	case ReduceTask:
		taskMetaData = ts.reduceTaskMap[task.TaskId]
	default:
		log.Panic("Cannot get unsupported task from TaskSet.")
	}
	taskMetaData.StartTime = time.Now()
	taskMetaData.Status = Working
}

func (ts *TaskSet) CompleteTask(task *Task) bool {
	var taskMetaData *TaskMetaData
	switch task.Type {
	case MapTask:
		taskMetaData = ts.mapTaskMap[task.TaskId]
	case ReduceTask:
		taskMetaData = ts.reduceTaskMap[task.TaskId]
	default:
		log.Panic("Cannot get unsupported task from TaskSet.")
	}
	if taskMetaData.Status == Done {
		log.Printf(
			"Task alreay completed, thus result abandoned. Task: %v\n", taskMetaData)
		return false
	}
	taskMetaData.Status = Done
	return true
}

func (ts *TaskSet) CheckComplete(taskType TaskType) bool {
	var m map[int]*TaskMetaData
	switch taskType {
	case MapTask:
		m = ts.mapTaskMap
	case ReduceTask:
		m = ts.reduceTaskMap
	default:
		log.Panic("Cannot check unsupported task type in TaskSet.")
	}
	for _, taskInfo := range m {
		if taskInfo.Status != Done {
			return false
		}
	}
	return true
}

func (ts *TaskSet) CheckTimeout(taskType TaskType) []*Task {
	var res []*Task
	var m map[int]*TaskMetaData
	switch taskType {
	case MapTask:
		m = ts.mapTaskMap
	case ReduceTask:
		m = ts.reduceTaskMap
	default:
		log.Panic("Cannot check unsupported task type in TaskSet.")
	}
	for _, taskInfo := range m {
		if taskInfo.Status == Working &&
			time.Since(taskInfo.StartTime) > TaskTimeout*time.Second {
			res = append(res, taskInfo.Task)
		}
	}
	return res
}
