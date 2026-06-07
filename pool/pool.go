package main

import (
    "context"
    "fmt"
    "time"
    "yourmodule/pool"
)

// EmailTask sends an email
type EmailTask struct {
    To      string
    Subject string
}

func (t *EmailTask) Process() error {
    fmt.Printf("sending email to %s: %s\n", t.To, t.Subject)
    time.Sleep(500 * time.Millisecond)
    return nil
}

// ResizeTask resizes an image
type ResizeTask struct {
    Filename string
    Width    int
    Height   int
}

func (t *ResizeTask) Process() error {
    fmt.Printf("resizing %s to %dx%d\n", t.Filename, t.Width, t.Height)
    time.Sleep(200 * time.Millisecond)
    return nil
}

// ReportTask generates a report
type ReportTask struct {
    ReportID string
}

func (t *ReportTask) Process() error {
    fmt.Printf("generating report %s\n", t.ReportID)
    time.Sleep(1 * time.Second)
    return nil
}

func main() {
    tasks := []pool.Task{
        &EmailTask{To: "a@example.com", Subject: "Hello"},
        &EmailTask{To: "b@example.com", Subject: "Hello"},
        &ResizeTask{Filename: "photo.jpg", Width: 800, Height: 600},
        &ResizeTask{Filename: "banner.png", Width: 1200, Height: 400},
        &ReportTask{ReportID: "Q3-2024"},
    }

    wp := pool.NewWorkPool(tasks, 3)
    errs := wp.Run(context.Background())

    if len(errs) > 0 {
        for _, err := range errs {
            fmt.Println("error:", err)
        }
    }
}