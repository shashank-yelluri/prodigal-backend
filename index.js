const express = require("express");
const assert = require("assert");
const MongoClient = require("mongodb").MongoClient;
const redis = require("redis");

var app = express();

var url =
  "mongodb+srv://prodigal_be_test_01:prodigaltech@test-01-ateon.mongodb.net/sample_training";

var dbName = "sample_training";
var db;

MongoClient.connect(url, function (err, client) {
  assert.equal(null, err);
  console.log("Connected successfully to Databse");

  db = client.db(dbName);
});

// const client = redis.createClient(6379);
const password = "P5DwCsZCrOSGXxVjpVnv4EWoa6QHFT22";

const client = redis.createClient({
  host: "redis-18567.c301.ap-south-1-1.ec2.cloud.redislabs.com",
  port: 18567,
  no_ready_check: true,
  auth_pass: password,
});

client
  .connect()
  .then(() => console.log("Connected successfully to Redis"))
  .catch((err) => console.log(err));

// Middlewares
app.param("student_id", async (req, res, next, id) => {
  const student = await db
    .collection("students")
    .findOne({ _id: parseInt(id) });

  req.student = student;
  next();
});

function cache(req, res, next) {
  client
    .get("students")
    .then((response) => {
      if (response === null) {
        next();
      } else {
        res.json(JSON.parse(response));
      }
    })
    .catch((err) => {
      console.log(err);
      next();
    });
}

app.get("/", (req, res) => {
  res.json({
    status: "success",
  });
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
// Task-01 Optimized version
app.get("/students/opt", cache, async (req, res) => {
  const payload = [];

  await db
    .collection("students")
    .find({})
    .forEach((student) => {
      payload.push({
        student_id: student._id,
        student_name: student.name,
      });
    });
  const result = JSON.stringify(payload);

  client
    .setEx("students", 3600, result)
    .then(() => console.log("Key set successfully!"))
    .catch((err) => console.log(err));
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

// Task-02 Optimized version
app.get("/student/:student_id/classes/opt", async (req, res) => {
  const student = req.student;

  const payload = {
    student_id: student._id,
    student_name: student.name,
    classes: [],
  };

  await db
    .collection("grades")
    .find({ student_id: student._id })
    .forEach((grade) => {
      const classs = {
        class_id: grade.class_id,
      };
      payload.classes.push(classs);
    });

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

// Task-03 Optimized version
app.get("/student/:student_id/performance/opt", async (req, res) => {
  const student = req.student;

  const payload = {
    student_id: student._id,
    student_name: student.name,
    classes: [],
  };

  await db
    .collection("grades")
    .aggregate([
      { $match: { student_id: student._id } },
      { $unwind: "$scores" },
      {
        $group: {
          _id: "$class_id",
          total: { $sum: "$scores.score" },
        },
      },
    ])
    .forEach((doc) => {
      payload.classes.push({
        class_id: doc._id,
        total_marks: Math.round(doc.total),
      });
    });

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

// Task-05 Optimized version
app.get("/class/:class_id/students/opt", async (req, res) => {
  const id = req.params["class_id"];

  const payload = {
    class_id: parseInt(id),
    students: [],
  };

  await db
    .collection("grades")
    .find({ class_id: parseInt(id) })
    .forEach((grade) => {
      payload.students.push({
        student_id: grade.student_id,
      });
    });

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

// Task-06 Optimized version
app.get("/class/:class_id/performance/opt", async (req, res) => {
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

app.listen(process.env.PORT || 3001, () =>
  console.log("Server started running on port 3001 !")
);
