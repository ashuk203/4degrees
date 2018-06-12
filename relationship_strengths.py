#File containing function that was run in the background processes of server

from shared_files.db import *
import time
from sqlalchemy import func
from sqlalchemy.orm import joinedload

import math

#Jobs more dated than the below threshold will be integrated into relationship strength factoring
#(here, jobs where more than 9 months have passed since their termination)
pastJobThreshold = 23650000

#Cutoff for determining how relationship strength is affected based on
#how much shared time the user and contact spent together in a job (closely associated with pastJobTreshold)
coworkerSharedTimeCutoff = 7884000

#If the last correspondence with this contact took place longer than the below threshhold
#will get a 'stale contact' relationship strength deduction
lastCorrespondenceCutoff = 31540000

def analyze_frequencies():
	print(str(time.time()) + ' - Starting strength determiner')
	
	#Sort 20 contacts with longest time since last relationship strength guess
	contactList = contact.query.options(joinedload(contact.contact_user,user.emails)).options(joinedload(contact.jobs)).order_by(contact.relationship_strength_time).limit(20)
	
	for eachContact in contactList:
		contactEmails = imported_email.query.with_entities(func.count(imported_email.contact_id).label('total'), func.max(imported_email.time).label('most_recent')).filter_by(contact_id=eachContact.id).first()
		contactMeetings = imported_meeting.query.with_entities(func.count(imported_meeting.contact_id).label('total'), func.max(imported_meeting.time).label('most_recent')).filter_by(contact_id=eachContact.id).first()
		contactInteractions = interaction.query.with_entities(func.count(interaction.contact_id).label('total'), func.max(interaction.time).label('most_recent')).filter_by(contact_id=eachContact.id).first()
		
		globalEmail = None
		if eachContact.contact_user is not None:
			emailList = [x.email.lower() for x in eachContact.contact_user.emails]
			globalEmail = global_email.query.filter(global_email.email.in_(emailList)).first()
		
		if globalEmail is not None:
			globalID = globalEmail.global_contact_id
			userJobs = global_job.query.filter_by(global_contact_id=globalID).all()
		else:
			userJobs = []

		#Comparing job histories to factor in common jobs
		allCommonJobIntervals = []
	
		for eachUserJob in userJobs:
			for eachContactJob in eachContact.jobs:
				adjustedUserEnd = (time.time() if eachUserJob.end_time == None else eachUserJob.end_time)
				adjustedContactEnd = (time.time() if eachContactJob.end_time == None else eachContactJob.end_time)

				adjustedUserStart = (time.time() if eachUserJob.start_time == None else eachUserJob.start_time)
				adjustedContactStart = (time.time() if eachContactJob.start_time == None else eachContactJob.start_time)

				#Negation of p1.start > p2.end OR p2.start > p1.end (i.e. there is no job intersection)
				# p1.start <= p2.end AND p2.start <= p1.end (i.e. there is a job intersection)
				if eachUserJob.company == eachContactJob.company and adjustedContactStart <= adjustedUserEnd and adjustedUserStart <= adjustedContactEnd:
					
					#Finds the intersection of the two jobs and appends it in [x, y] form to allCommonJobIntervals
					commonStartTime = max(adjustedUserStart, adjustedContactStart)
					
					commonEndTime = min(adjustedUserEnd, adjustedContactEnd)
					allCommonJobIntervals.append([commonStartTime, commonEndTime])

		relationshipStrength = get_relationship_points(getattr(contactEmails, 'total', 0), getattr(contactMeetings, 'total', 0), getattr(contactInteractions, 'total', 0))
		sameJobTime = previous_total_job_time(allCommonJobIntervals)

		emailMostRecent = 0 if getattr(contactEmails, 'most_recent', 0) == None else getattr(contactEmails, 'most_recent', 0)
		meetingMostRecent = 0 if getattr(contactMeetings, 'most_recent', 0) == None else getattr(contactMeetings, 'most_recent', 0)
		interactionMostRecent = 0 if getattr(contactInteractions, 'most_recent', 0) == None else getattr(contactInteractions, 'most_recent', 0)

		lastCorrespondence = max(emailMostRecent, meetingMostRecent, interactionMostRecent)
		
		if sameJobTime >= coworkerSharedTimeCutoff:
			relationshipStrength *= 0.8
		
		if (time.time() - lastCorrespondence) >= lastCorrespondenceCutoff:
			relationshipStrength -= 1.5
			
		relationshipStrength = max(relationshipStrength, 1)
		
		#Updating relationship strength and time of update
		eachContact.relationship_strength = relationshipStrength
		eachContact.relationship_strength_time = time.time()

	db.session.commit()
	print(str(time.time()) + ' - Ending strength determiner')


#Takes in disjoint sets of intervals representing shared job time with users
#and returns the total amount of shared job time greater than 9 months ago
def previous_total_job_time(commonJobIntervals):
	totalJobSharedTime = 0

	for eachInterval in commonJobIntervals:
		if time.time() - eachInterval[1] > pastJobThreshold:
			totalJobSharedTime += eachInterval[1] - eachInterval[0]

	return totalJobSharedTime


#Formula assigning relationship strength on a scale of 0-10 based on
#r = (x/40) / root(1 + (x / 40)^2)
#x = (0.3 * emails) + (3 * meetings) + (2 * interactions)
#r-score = 10 * r
def get_relationship_points(numEmails, numMeetings, numInteraction):
	rawScore = ((0.3*numEmails) + (3*numMeetings) + (2*numInteraction)) / 40
	score = (rawScore)/pow(1 + pow(rawScore,2), 0.5)
	
	return min(10, (10*score))



