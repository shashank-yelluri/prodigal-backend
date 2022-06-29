const express = require("express");
const assert = require("assert");
const MongoClient = require("mongodb").MongoClient;

var app = express();

var url =
  "mongodb+srv://prodigal_be_test_01:prodigaltech@test-01-ateon.mongodb.net/sample_training";

var dbName = "sample_training";
var db;

MongoClient.connect(url, function (err, client) {
  assert.equal(null, err);
  console.log("Connected successfully to DB");

  db = client.db(dbName);
});

// Task-01
app.get("/students", async (req, res) => {
  const students = await db.collection("students").find({}).toArray();

  const payload = [];

  for (let index = 0; index < students.length; index++) {
    const record = {
      student_id: students[index]["_id"],
      student_name: students[index]["name"],
    };
    payload.push(record);
  }

  res.json(payload);
});

// Task-02
app.get("/student/:student_id/classes", async (req, res) => {
  const id = req.params["student_id"];

  const student = await db
    .collection("students")
    .find({ _id: parseInt(id) })
    .toArray();

  const payload = {
    student_id: student[0]["_id"],
    student_name: student[0]["name"],
    classes: [],
  };

  const grades = await db
    .collection("grades")
    .find({ student_id: parseInt(id) })
    .toArray();

  for (let index = 0; index < grades.length; index++) {
    const record = {
      class_id: grades[index]["class_id"],
    };

    payload.classes.push(record);
  }

  res.json(payload);
});

// Task-03
app.get("/student/:student_id/performance", async (req, res) => {
  const id = req.params["student_id"];

  const student = await db
    .collection("students")
    .find({ _id: parseInt(id) })
    .toArray();

  const payload = {
    student_id: student[0]["_id"],
    student_name: student[0]["name"],
    classes: [],
  };

  const grades = await db
    .collection("grades")
    .find({ student_id: parseInt(id) })
    .toArray();

  for (let index = 0; index < grades.length; index++) {
    let scores = grades[index]["scores"];
    let total = 0;
    for (let j = 0; j < scores.length; j++) {
      total = total + scores[j]["score"];
    }
    const record = {
      class_id: grades[index]["class_id"],
      total_marks: Math.round(total),
    };

    payload.classes.push(record);
  }

  res.json(payload);
});

// Task-04
app.get("/classes", async (req, res) => {
  const classes = await db.collection("grades").distinct("class_id");

  const paylod = [];

  for (let index = 0; index < classes.length; index++) {
    const record = {
      class_id: classes[index],
    };

    paylod.push(record);
  }

  res.json(paylod);
});

// Task-05
app.get("/class/:class_id/students", async (req, res) => {
  const id = req.params["class_id"];

  const grades = await db
    .collection("grades")
    .find({ class_id: parseInt(id) })
    .toArray();

  const payload = {
    class_id: parseInt(id),
    students: [],
  };

  for (let index = 0; index < grades.length; index++) {
    const student = await db
      .collection("students")
      .find({ _id: grades[index]["student_id"] })
      .toArray();

    const data = {
      student_id: grades[index]["student_id"],
      student_name: student[0]["name"],
    };

    payload.students.push(data);
  }

  res.json(payload);
});

// Task-06
app.get("/class/:class_id/performance", async (req, res) => {
  const id = req.params["class_id"];

  const grades = await db
    .collection("grades")
    .find({ class_id: parseInt(id) })
    .toArray();

  const payload = {
    class_id: parseInt(id),
    students: [],
  };

  for (let index = 0; index < grades.length; index++) {
    const student = await db
      .collection("students")
      .find({ _id: grades[index]["student_id"] })
      .toArray();

    const marks = await db
      .collection("grades")
      .find({ class_id: parseInt(id), student_id: grades[index]["student_id"] })
      .toArray();

    const scores = marks[0]["scores"];

    let total = 0;
    for (let j = 0; j < scores.length; j++) {
      total = total + scores[j]["score"];
    }

    const data = {
      student_id: grades[index]["student_id"],
      student_name: student[0]["name"],
      total_marks: Math.round(total),
    };

    payload.students.push(data);
  }

  res.json(payload);
});

// Task-07
app.get("/class/:class_id/student/:student_id", async (req, res) => {
  const class_id = req.params["class_id"];
  const student_id = req.params["student_id"];

  const student = await db
    .collection("students")
    .find({ _id: parseInt(student_id) })
    .toArray();

  const payload = {
    class_id: parseInt(class_id),
    student_id: parseInt(student_id),
    student_name: student[0]["name"],
    marks: [],
  };

  const common = await db
    .collection("grades")
    .find({ class_id: parseInt(class_id), student_id: parseInt(student_id) })
    .toArray();

  const scores = common[0]["scores"];
  let total = 0;

  for (let index = 0; index < scores.length; index++) {
    const item = {
      type: scores[index]["type"],
      marks: Math.round(scores[index]["score"]),
    };
    payload.marks.push(item);

    total = total + scores[index]["score"];
  }

  const final = {
    type: "total",
    marks: Math.round(total),
  };

  payload.marks.push(final);

  res.json(payload);
});

app.listen(3001, () => console.log("Server started running on port 3001 !"));