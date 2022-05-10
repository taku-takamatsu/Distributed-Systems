package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

func ToA2RejectP1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... p1 entered Accept phase")
		}
		return valid
	}
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s1 := s.Nodes()["s1"].(*Server)
		valid := s3.proposer.Phase == Propose && s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... p3 entered Propose phase")
		}
		return valid
	}
	p2SeesP3Propose := func(s *base.State) bool {
		p3 := s.Nodes()["s3"].(*Server)
		p2 := s.Nodes()["s2"].(*Server)
		valid := p3.proposer.N > p2.n_p
		if valid {
			fmt.Println("... a2 saw P3 Propose phase")
		}
		return valid
	}
	// p3 enters accept phase
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept
		if valid {
			fmt.Println("... p3 entered Accept phase")
		}
		return valid
	}

	p1ReceiveReject := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 1
		if valid {
			fmt.Println("... P1 received 1 response")
		}
		return valid
	}

	return []func(s *base.State) bool{
		p1PreparePhase,
		p1AcceptPhase,
		p3PreparePhase,
		p2SeesP3Propose,
		p3AcceptPhase,
		p1ReceiveReject}
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	// p3 enters decide phase
	p3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.agreedValue == "v3"
		if valid {
			fmt.Println("... p3 entered Decide phase")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p3DecidePhase}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s1 := s.Nodes()["s1"].(*Server)
		valid := s3.proposer.Phase == Propose && s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... p3 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... p1 entered Accept phase")
		}
		return valid
	}
	p1ReceiveReject := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 1 && s1.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... P1 received 1 response")
		}
		return valid
	}
	p1ReceiveReject2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 2 && s1.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... P1 received 2 response")
		}
		return valid
	}
	p1ReceiveReject3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 3 && s1.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... P1 received 3 response")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p1AcceptPhase,
		p3PreparePhase,
		p1ReceiveReject,
		p1ReceiveReject2,
		p1ReceiveReject3}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p1ReceiveProposeResponse := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.ResponseCount == 1
		if valid {
			fmt.Println("... p1 received 1 Propose response")
		}
		return valid
	}
	p1ReceiveProposeResponse2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.ResponseCount == 2
		if valid {
			fmt.Println("... p1 received 2 Propose response")
		}
		return valid
	}
	p1ReceiveProposeResponse3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.ResponseCount == 3
		if valid {
			fmt.Println("... p1 received 3 Propose response")
		}
		return valid
	}
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept
		if valid {
			fmt.Println("... p3 entered Accept phase")
		}
		return valid
	}
	p3ReceiveReject := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.ResponseCount == 1 && s3.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... P3 received 1 response")
		}
		return valid
	}
	p3ReceiveReject2 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.ResponseCount == 2 && s3.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... P3 received 2 response")
		}
		return valid
	}
	p1ReceiveReject3 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.ResponseCount == 3 && s3.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... P3 received 3 response")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p1ReceiveProposeResponse,
		p1ReceiveProposeResponse2,
		p1ReceiveProposeResponse3,
		p3AcceptPhase,
		p3ReceiveReject,
		p3ReceiveReject2,
		p1ReceiveReject3}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	return NotTerminate1() // Dueling proposers - same as NotTerminate1
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		s1 := s.Nodes()["s1"].(*Server)
		valid := s3.proposer.Phase == Propose && s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... p3 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... p1 entered Accept phase")
		}
		return valid
	}
	p1ReceiveReject := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 1 && s1.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... P1 received 1 response")
		}
		return valid
	}
	p1ReceiveReject2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 2 && s1.proposer.SuccessCount == 0
		if valid {
			fmt.Println("... P1 received 2 response")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p1AcceptPhase,
		p3PreparePhase,
		p1ReceiveReject,
		p1ReceiveReject2}
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept
		if valid {
			fmt.Println("... p3 entered Accept phase")
		}
		return valid
	}
	p3ReceiveAcceptOk := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.SuccessCount == 1
		if valid {
			fmt.Println("... P3 received 1 response")
		}
		return valid
	}
	p3ReceiveAcceptOk2 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.SuccessCount == 2
		if valid {
			fmt.Println("... P3 received 2 response")
		}
		return valid
	}

	return []func(s *base.State) bool{
		p3AcceptPhase,
		p3ReceiveAcceptOk,
		p3ReceiveAcceptOk2}
}
