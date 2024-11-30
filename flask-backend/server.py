from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy import text

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Models
class Student(db.Model):
    __tablename__ = 'students'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)

class Meeting(db.Model):
    __tablename__ = 'meetings'
    id = db.Column(db.Integer, primary_key=True)
    topic = db.Column(db.String(100), nullable=False, index=True)  # Index on topic
    date = db.Column(db.Date, nullable=False, index=True)          # Index on date
    duration = db.Column(db.Integer)
    invited_students = db.Column(db.Integer)
    accepted_invitations = db.Column(db.Integer)
    meeting_type = db.Column(db.String(50), nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "topic": self.topic,
            "date": self.date.isoformat(),
            "duration": self.duration,
            "invited_students": self.invited_students,
            "accepted_invitations": self.accepted_invitations,
            "meeting_type": self.meeting_type
        }

class MeetingOrganizers(db.Model):
    __tablename__ = 'meeting_organizers'
    id = db.Column(db.Integer, primary_key=True)
    meeting_id = db.Column(db.Integer, db.ForeignKey('meetings.id'), nullable=False)
    student_id = db.Column(db.Integer, db.ForeignKey('students.id'), nullable=False)

    __table_args__ = (
        db.Index('idx_meeting_id', 'meeting_id'),
        db.Index('idx_student_id', 'student_id')
    )

# Initialize database and add sample data if empty
with app.app_context():
    db.create_all()
    if not Meeting.query.first():
        sample_meetings = [
            Meeting(topic="Project Kickoff", date=datetime(2024, 11, 1), duration=60, invited_students=10, accepted_invitations=8, meeting_type="Kickoff"),
            Meeting(topic="Weekly Sync", date=datetime(2024, 11, 2), duration=45, invited_students=15, accepted_invitations=12, meeting_type="Sync")
        ]
        db.session.add_all(sample_meetings)
        db.session.commit()

# Routes for Meeting Management
@app.route('/meetings', methods=['POST'])
def add_meeting():
    data = request.json
    try:
        new_meeting = Meeting(
            topic=data['topic'],
            date=datetime.strptime(data['date'], '%Y-%m-%d'),
            duration=data.get('duration', 60),
            invited_students=data.get('invited_students', 10),
            accepted_invitations=data.get('accepted_invitations', 8),
            meeting_type=data['meeting_type']
        )
        db.session.add(new_meeting)
        db.session.commit()
        return jsonify({"message": "Meeting added successfully", "id": new_meeting.id}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to add meeting", "details": str(e)}), 400

@app.route('/meetings/<int:id>', methods=['PUT'])
def edit_meeting(id):
    data = request.json
    try:
        meeting = Meeting.query.get(id)
        if meeting:
            meeting.topic = data.get('topic', meeting.topic)
            meeting.date = datetime.strptime(data['date'], '%Y-%m-%d') if 'date' in data else meeting.date
            meeting.duration = data.get('duration', meeting.duration)
            meeting.invited_students = data.get('invited_students', meeting.invited_students)
            meeting.accepted_invitations = data.get('accepted_invitations', meeting.accepted_invitations)
            meeting.meeting_type = data.get('meeting_type', meeting.meeting_type)
            db.session.commit()
            return jsonify(meeting.to_dict()), 200
        return jsonify({"message": "Meeting not found"}), 404
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to update meeting", "details": str(e)}), 400

@app.route('/meetings/<int:id>', methods=['DELETE'])
def delete_meeting(id):
    try:
        meeting = Meeting.query.get(id)
        if meeting:
            db.session.delete(meeting)
            db.session.commit()
            return jsonify({"message": "Meeting deleted successfully"}), 200
        return jsonify({"message": "Meeting not found"}), 404
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": "Failed to delete meeting", "details": str(e)}), 400

@app.route('/meetings', methods=['GET'])
def get_meetings():
    meetings = Meeting.query.all()
    return jsonify([meeting.to_dict() for meeting in meetings])

@app.route('/meetings/report', methods=['GET'])
def generate_report():
    date = request.args.get('date')  # Date filter

    try:
        # Use Serializable isolation level for critical data consistency
        with db.engine.connect().execution_options(isolation_level="SERIALIZABLE") as connection:
            query = text("""
                SELECT topic, 
                    AVG(duration) AS average_duration, 
                    AVG(invited_students) AS average_invited_students, 
                    AVG(accepted_invitations) AS average_accepted_invitations,
                    COALESCE(SUM(accepted_invitations) * 100 / NULLIF(SUM(invited_students), 0), 0) AS average_attendance_rate
                FROM meetings
                WHERE date = :date
                GROUP BY topic
            """)
            result = connection.execute(query, {"date": date}).fetchall()

        report = [
            {
                "topic": row.topic,
                "average_duration": row.average_duration,
                "average_invited_students": row.average_invited_students,
                "average_accepted_invitations": row.average_accepted_invitations,
                "average_attendance_rate": row.average_attendance_rate
            }
            for row in result
        ]
        return jsonify({"report": report})
    except Exception as e:
        return jsonify({"error": "Failed to generate report", "details": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
