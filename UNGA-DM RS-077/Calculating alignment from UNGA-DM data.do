/********************************************************************************/
/*"Calculating alignment from UNGA-DM data.do"  December 1, 2024                */
/* Christopher Kilby (christopher.kilby@villanova.edu)                          */
/*                                                                              */
/* Creates file of annual alignments with US either on all votes (1946-2022)    */
/* or just US-important votes (1983-2022). Note: For a handful of countries,    */
/* there are a few missing values for aligment on US-important votes because the*/
/* country was suspended for every important vote that year.                    */
/*                                                                              */
/* If you use these data, please cite:                                          */
/* Fjelstul, Joshua, Simon Hug & Christopher Kilby (forthcoming).               */
/* "Decision-Making in the United Nations General Assembly: A Comprehensive     */
/* Database of Resolutions, Decisions, and Votes."                              */
/* The Review of International Organizations.                                   */
/* and                                                                          */
/* Fjelstul, Joshua, Simon Hug & Christopher Kilby (2023).                      */
/* "UNGA-DM Codebook, v 2023.2." https://unvotes.unige.ch/download_file/1/1.    */
/*                                                                              */
/* Variables created:                                                           */
/* S_USA: alignment with US on all roll-call votes. Like S-score but 0-1 scale. */
/* S_USA_important: alignment with US on US-important votes.                    */
/*                                                                              */
/* Note: this calculation treats absences (not voting) the same as abstentions  */
/* unless the country was not voted due to suspension.                          */
/********************************************************************************/

#delimit ;
import delimited "All_Votes_RS-077.csv", varnames(1) clear;
gen year=year(date(meeting_date,"DMY"));
drop if year>2022;  /*Data for 2023 are incomplete since RS 77 ends in September*/
preserve;
  keep if member_state=="United States";
  rename original_vote US_vote;
  keep decision_id US_vote;
  save _tmp, replace;
restore;
merge m:1 decision_id using _tmp, nogen;
erase _tmp.dta;
gen SameAsUSA=(original_vote==US_vote);
replace SameAsUSA=.5 if ((original_vote=="abstaining" | original_vote=="not voting") & (US_vote=="against" | US_vote=="in favor")) | 
                        ((US_vote=="abstaining" | US_vote=="not voting") & (original_vote=="against" | original_vote=="in favor"));
replace SameAsUSA=. if original_vote=="not voting (suspended)" | US_vote=="not voting (suspended)";
gen SameAsUSA_important=SameAsUSA if important=="important";
collapse (mean) S_USA=SameAsUSA S_USA_important=SameAsUSA_important, by(member_state member_state_id year) fast;
drop if missing(S_USA) & missing(S_USA_important);
export delimited "UNGA-DM_RS-077 alignment with US.csv", replace;
